use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    time::{SystemTime, UNIX_EPOCH},
};
fn main() {
    let mut se = StorageEngine::new(&"db.wal".to_string());
    loop {
        let mut cmd = String::new();
        io::stdin()
            .read_line(&mut cmd)
            .expect("Failed to read line");

        // Usage : [SET/GET/DEL <KEY> <VAL>]
        let cmd: Vec<String> = cmd.trim().split_whitespace().map(String::from).collect();

        if cmd.len() < 1 {
            println!("Usage : [SET/GET/DEL/SHOWKEYS <KEY> <VAL>]")
        }

        let action = cmd.get(0).unwrap();

        // Validate action first
        let action_lower = action.to_ascii_lowercase();
        if action_lower != "set"
            && action_lower != "get"
            && action_lower != "del"
            && action_lower != "showkeys"
        {
            println!("Supported actions: SET, GET, DEL, SHOWKEYS")
        }

        // Get key if provided
        let key = match cmd.get(1) {
            Some(key) => key.clone(),
            None => {
                if action_lower != "showkeys" {
                    println!(
                        "Expected key for action: {}",
                        action_lower.to_ascii_uppercase()
                    );
                }
                String::new()
            }
        };

        // Get value if provided
        let val = match cmd.get(2) {
            Some(value) => value.clone(),
            None => {
                if action_lower == "set" {
                    println!("Expected value for SET action");
                }
                String::new() // Empty string for GET/DEL operations
            }
        };

        // Execute the action
        match action_lower.as_str() {
            "set" => {
                se.set(key.clone(), val.clone());
                println!(">> {} = {}", key, val);
            }
            "get" => match se.get(key.clone()) {
                Some(value) => println!(">> {} = {}", key, value),
                None => println!("Key '{}' not found", key),
            },
            "del" => {
                se.delete(key.clone());
                println!("DEL {}", key);
            }
            "showkeys" => {
                todo!("SHOWKEYS not yet supported")
            }
            _ => unreachable!(),
        }
    }
}

// Log-based storage engine
// Uses Write-ahead Log with periodic cleanup.
struct StorageEngine {
    db_file: File,
    // map key start to file position
    key_position_map: HashMap<String, u64>,
    sequence_number: i32,
    // WAL Line format:
    // [16B Unix Millis Timestamp] [4B key len] [ 4B val len ] [ key bytes ] [ val_bytes ]
    // If val len is 0 bytes, we assume it's deleted.
}

struct LogEntry {
    timestamp: SystemTime,
    key: String,
    val: String,
}

impl LogEntry {
    pub fn new(key: String, val: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            key: key,
            val: val,
        }
    }
    pub fn to_binary_log(&mut self) -> Vec<u8> {
        let binary_key = self.key.as_bytes();
        let binary_val = self.val.as_bytes();

        // Prepare the data to write
        let key_len = (binary_key.len() as u32).to_be_bytes();
        let val_len = (binary_val.len() as u32).to_be_bytes();

        // 16 B timestamp + 4 B key len + 4 B val len + actual key + actual val
        let mut log_buf = vec![];
        let curr_time = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Write timestamp (16 bytes)
        log_buf.write(&curr_time.to_be_bytes()).unwrap();
        // Write key length (4 bytes)
        log_buf.write(&key_len).unwrap();
        // Write value length (4 bytes)
        log_buf.write(&val_len).unwrap();

        // Write key
        log_buf.write(&binary_key).unwrap();
        // Write val
        log_buf.write(&binary_val).unwrap();

        return log_buf;
    }
}

impl StorageEngine {
    pub fn new(db_file: &String) -> Self {
        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file)
            .unwrap();

        let mut storage_engine = StorageEngine {
            db_file: wal_file,
            key_position_map: HashMap::new(),
            sequence_number: 0,
        };

        storage_engine.load_key_pos_map_from_file();

        // Return the initialized storage_engine, not a new empty one
        return storage_engine;
    }

    fn load_key_pos_map_from_file(&mut self) {
        let mut mp: HashMap<String, u64> = HashMap::new();

        // Get the file size
        let file_size = self.db_file.metadata().unwrap().len();

        let mut current_pos: u64 = 0;

        // Read through the entire file
        while current_pos < file_size {
            // Read timestamp (16 bytes) - we don't need it for the map
            let mut _timestamp = [0u8; 16];
            if self
                .db_file
                .read_exact_at(&mut _timestamp, current_pos)
                .is_err()
            {
                break;
            }

            // Read key length (4 bytes)
            let mut key_len_bytes = [0u8; 4];
            if self
                .db_file
                .read_exact_at(&mut key_len_bytes, current_pos + 16)
                .is_err()
            {
                break;
            }
            let key_len = u32::from_be_bytes(key_len_bytes) as u64;

            // Read value length (4 bytes)
            let mut val_len_bytes = [0u8; 4];
            if self
                .db_file
                .read_exact_at(&mut val_len_bytes, current_pos + 20)
                .is_err()
            {
                break;
            }
            let val_len = u32::from_be_bytes(val_len_bytes) as u64;

            // Read the key
            let mut key_buffer = vec![0u8; key_len as usize];
            if self
                .db_file
                .read_exact_at(&mut key_buffer, current_pos + 24)
                .is_err()
            {
                break;
            }
            let key = String::from_utf8(key_buffer).unwrap();

            // Update the map with this entry's position
            // This will naturally overwrite older entries with newer ones
            mp.insert(key, current_pos);
            self.sequence_number += 1;

            // Move to the next entry
            // Entry size = 16 (timestamp) + 4 (key_len) + 4 (val_len) + key_len + val_len
            current_pos += 24 + key_len + val_len;
        }

        self.key_position_map = mp
    }

    pub fn compact(&mut self) {
        todo!("not fully implemented");
        let mut tmp_engine = StorageEngine::new(&"/tmp/tmp_waldb".to_string());

        for (key, val) in self.key_position_map.iter() {
            let v = self.get(key.to_string()).unwrap_or(String::new());
            tmp_engine.set(key.to_string(), v);
        }
    }

    pub fn set(&mut self, key: String, val: String) {
        let mut entry = LogEntry::new(key.clone(), val);

        let binary_log_entry = entry.to_binary_log();

        // Get current file length to append at the end
        let file_len = self.db_file.metadata().unwrap().len();
        self.db_file
            .write_all_at(&binary_log_entry, file_len)
            .unwrap();
        self.sequence_number += 1;

        // Update the key position map
        self.key_position_map.insert(key, file_len);
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        let key_start_pos = match self.key_position_map.get(&key) {
            Some(pos) => pos,
            None => return None, // Key doesn't exist, return None instead of panicking
        };

        // Skip timestamp (16 bytes) and read key length
        let mut key_len = [0u8; 4];
        self.db_file
            .read_exact_at(&mut key_len, *key_start_pos + 16)
            .unwrap();

        // Read value length
        let mut val_len = [0u8; 4];
        self.db_file
            .read_exact_at(&mut val_len, *key_start_pos + 20)
            .unwrap();

        let key_len_u32 = u32::from_be_bytes(key_len);
        let val_len_u32 = u32::from_be_bytes(val_len);
        if val_len_u32 == 0 {
            return None;
        }

        // Skip the key data and read the value
        let mut val_buffer = vec![0u8; val_len_u32 as usize];
        self.db_file
            .read_exact_at(&mut val_buffer, *key_start_pos + 24 + key_len_u32 as u64)
            .unwrap();

        return Some(String::from_utf8(val_buffer).unwrap());
    }

    pub fn delete(&mut self, key: String) {
        self.set(key, String::new());
    }
}
