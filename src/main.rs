use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use nom::bytes;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0;
    let mut shift = 0;
    let mut bytes_read = 0;
    let mut bs = bytes.iter().copied();
    loop {
        let byte = bs.next().ok_or(anyhow!("no next byte"))?;
        bytes_read += 1;
        if bytes_read == 9 {
            result = (result << shift) | u64::from(byte);
            break;
        }
        result = (result << shift) | u64::from(byte & 0b0111_1111);
        shift += 7;
        if byte & 0b1000_0000 == 0 {
            break;
        }
    }

    Ok((result, bytes_read))
}

#[derive(Debug)]
enum DataType {
    Null,
    String,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Unknown,
}

#[derive(Debug)]
enum DataValue {
    Null,
    String(String),
    Int(i8),
    Unknown,
}

fn get_type_definition(type_code: u64) -> (DataType, usize) {
    match type_code {
        0 => (DataType::Null, 0),
        1 => (DataType::Int8, 1),
        2 => (DataType::Int16, 2),
        3 => (DataType::Int24, 3),
        4 => (DataType::Int32, 4),
        5 => (DataType::Int48, 6),
        6 => (DataType::Int64, 8),
        _ => {
            if type_code % 2 == 0 {
                let size = ((type_code - 12) / 2) as usize;

                (DataType::Unknown, size)
            } else {
                let size = ((type_code - 13) / 2) as usize;

                (DataType::String, size)
            }
        }
    }
}

#[derive(Debug)]
struct DbPage {
    rows: Vec<Vec<DataValue>>,
}

#[derive(Debug)]
struct DbTableConfig {
    table_name: String,
    page_number: i8,
}

#[derive(Debug)]
struct Db {
    page_size: u16,
    num_of_tables: u16,
    file_path: String,
}

impl Db {
    fn new(file_path: &str) -> Result<Self> {
        let mut file = File::open(file_path)?;
        // includes db header 100 + first page header 8
        let mut bytes_to_read = [0; 108];
        file.read_exact(&mut bytes_to_read)?;

        let page_size = u16::from_be_bytes([bytes_to_read[16], bytes_to_read[17]]);
        let num_of_tables = u16::from_be_bytes([bytes_to_read[103], bytes_to_read[104]]);

        Ok(Self {
            file_path: file_path.to_owned(),
            page_size,
            num_of_tables,
        })
    }

    fn get_page(&self, page_number: u16) -> Result<DbPage> {
        let start_offset = if page_number == 1 { 100 } else { 0 };

        // align since pages 1 based
        let page_number = page_number - 1;

        let mut file = File::open(&self.file_path)?;
        let mut bytes_to_read = vec![0; (self.page_size - start_offset) as usize];
        file.seek(SeekFrom::Start(
            (page_number * self.page_size + start_offset) as u64,
        ))?;
        file.read_exact(&mut bytes_to_read)?;

        let num_of_cells = u16::from_be_bytes([bytes_to_read[3], bytes_to_read[4]]);

        let mut rows: Vec<Vec<DataValue>> = vec![];
        let cell_data_start_length = 8;
        for i in 0..num_of_cells {
            // cell offset on the bottom of the page
            let cell_start_offset = u16::from_be_bytes([
                bytes_to_read[cell_data_start_length + (i * 2) as usize],
                bytes_to_read[cell_data_start_length + (i * 2 + 1) as usize],
            ]);

            let mut row_offset: usize = (cell_start_offset - start_offset) as usize;
            let (_cell_size, offset) = decode_varint(&bytes_to_read[row_offset..])?;
            row_offset += offset;

            let (_row_id, offset) = decode_varint(&bytes_to_read[row_offset..])?;
            row_offset += offset;

            let (header_size, offset) = decode_varint(&bytes_to_read[row_offset..])?;

            let header_end_offset = row_offset + header_size as usize;
            row_offset += offset;

            let mut type_definitions: Vec<(DataType, usize)> = vec![];
            while row_offset < header_end_offset {
                let (content, offset) =
                    decode_varint(&bytes_to_read[row_offset..header_end_offset])?;
                row_offset += offset;

                let type_definition = get_type_definition(content);
                type_definitions.push(type_definition);
            }

            let mut row_data: Vec<DataValue> = vec![];
            let mut values_offset = header_end_offset;
            for type_definition in type_definitions {
                let value_length = type_definition.1;
                let value_bytes = &bytes_to_read[values_offset..values_offset + value_length];

                match type_definition.0 {
                    DataType::String => {
                        let value = std::str::from_utf8(value_bytes)?;
                        row_data.push(DataValue::String(value.to_owned()));
                    }
                    DataType::Int8 => {
                        let value_bytes =
                            &bytes_to_read[values_offset..values_offset + value_length];
                        let value = i8::from_be(value_bytes[0] as i8);
                        row_data.push(DataValue::Int(value));
                    }
                    DataType::Null => row_data.push(DataValue::Null),
                    _ => {
                        row_data.push(DataValue::Unknown);
                    }
                }

                values_offset += value_length;
            }

            rows.push(row_data);
        }

        Ok(DbPage { rows })
    }

    fn get_table_configs(&self) -> Result<Vec<DbTableConfig>> {
        let page = self.get_page(1)?;
        let name_column_index = 2;
        let page_column_index = 3;

        Ok(page
            .rows
            .iter()
            .filter_map(|row| {
                let table_name = match row.get(name_column_index).unwrap_or(&DataValue::Unknown) {
                    DataValue::String(val) => val.clone(),
                    _ => "".into(),
                };
                let page_number = match row.get(page_column_index).unwrap_or(&DataValue::Unknown) {
                    DataValue::Int(val) => val.clone(),
                    _ => 0,
                };

                if table_name == "" || page_number == 0 {
                    return None;
                }

                Some(Ok(DbTableConfig {
                    table_name,
                    page_number,
                }))
            })
            .collect::<Result<Vec<DbTableConfig>>>()?)
    }

    fn get_table_names(&self) -> Result<Vec<String>> {
        let table_configs = self.get_table_configs()?;

        return Ok(table_configs
            .iter()
            .map(|config| config.table_name.clone())
            .collect::<Vec<String>>());
    }

    fn get_table_page(&self, table_name: &str) -> Result<DbPage> {
        let configs = self.get_table_configs()?;
        let config = configs
            .iter()
            .find(|config| config.table_name == table_name)
            .ok_or(anyhow!("No data for table"))?;

        self.get_page(config.page_number as u16)
    }
}

// TODO: process all ?
fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db = Db::new(&args[1])?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size);
            println!("number of tables: {}", db.num_of_tables);
        }
        ".tables" => {
            println!("{:?}", db.get_table_names()?.join(" "));
        }
        other => {
            // TODO: case sensitivity
            if other.starts_with("SELECT") {
                let table_name = other.split_whitespace().last();
                if let Some(table_name) = table_name {
                    let table_page = db.get_table_page(table_name)?;
                    println!("{}", table_page.rows.len());
                } else {
                    bail!("Requested table does not exist {}", command);
                }
            } else {
                bail!("Missing or invalid command passed: {}", command);
            }
        }
    }

    Ok(())
}
