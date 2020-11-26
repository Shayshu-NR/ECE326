/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

use packet::{Command, Request, Response, Value};
use schema::{Table};
use std::collections::HashMap;
 
/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */

// Use a Row struct as the hash key for the db
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Row{
    pub row_id : i64
}

// Implement the init for the hasable row struct
impl Row {
    fn new(r_id : &i64) -> Row {
        Row {row_id : *r_id}
    }
}

#[derive(Debug, Clone)]
pub struct Rowinfo{
    pub info : Vec<Value>,
    pub version : i64
}

impl Rowinfo{
    fn new(r_info : Vec<Value>, r_version : i64) -> Rowinfo{
        Rowinfo {info : r_info, version : r_version}
    }
}

/* 
* DB consists of a vector of tables, which contain column data
* And a hash table maping the table name, and row id (both as strings) to
* a vector of values
* 
DB{
    [Users, Account, ...],
    {
        Row{
            table_name : "User", 
            row_id: "1"
        }
        : 
        Rowinfo{
            info : [Text("John"), Text("Doe"), Float(6.0), Integer(20)],
            version : 0
        },
        .
        .
        .
        Row{
            table_name : "Account", 
            row_id: "1"
        }
        : 
        Rowinfo{
            info : [Foreign(1), Text("Savings"), Flot(5000.0)],
            version : 0
        },
    }
}
*/ 
#[derive(Debug, Clone)]
pub struct Database { 
    pub schema: Vec<Table>,
    pub tables: Vec<HashMap<Row, Rowinfo>>, 
    pub r_ids : Vec<i64>
}

/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: & mut Database) 
    -> Response  
{       
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => 
            handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) => 
             handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) => 
            handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };
    
    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

/*
 * TODO: Implment these EasyDB functions
 */
fn check_type(passed_value : i32, col_type : i32) -> bool{
    if passed_value != col_type {
        return false;
    }
    else{
        return true;
    }
}

fn handle_insert(db: & mut Database, table_id: i32, values: Vec<Value>) 
    -> Result<Response, i32> 
{   
    // First make sure that the correct amount of values was supplied...
    // And make sure the table actually exists
    let mut table_exists = false;
    let mut id : usize = 0;

    for i in &mut db.schema { 
        if i.t_id == table_id {
            table_exists = true;

            // Make sure the correct number of cols was supplied
            if i.t_cols.len() != values.len(){
                return Err(Response::BAD_ROW);
            }
            
            break;
        }
        id = id + 1;
    }

    // Check if the table exists
    if !table_exists {
        return Err(Response::BAD_TABLE);
    }

    // Make sure that ever value has the correct column type
    let mut col_id : usize = 0;
    for i in &values{
        match i {
            Value::Null => if !check_type(0, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(i) => if !check_type(1, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Float(i) => if !check_type(2, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Text(i) => if !check_type(3, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(i) => 
                if !check_type(4, db.schema[id].t_cols[col_id].c_type) {
                    return Err(Response::BAD_VALUE);
                }
                // Make sure the foreign key exists...
                else{
                    // First get the foreign table id...
                    let f_key = (db.schema[id].t_cols[col_id].c_ref - 1) as usize;

                    let search = Row::new(&i);
                    // Search for the key and return the version and values
                    match db.tables[f_key].get(&search) {
                        Some(x) => (),
                        _ => return Err(Response::BAD_FOREIGN),
                    }
                },
        }
        col_id = col_id + 1;
    }

    //Start by making the row struct...
    let hash_r_id = db.r_ids[id] + 1;
    
    let new_row = Row::new(&hash_r_id);
    let new_row_info = Rowinfo::new(values, 1);
    db.r_ids[id] = db.r_ids[id] + 1;
    db.tables[id].insert(new_row, new_row_info);

    return Ok(Response::Insert(hash_r_id, 1));
}

// Shayshu
fn handle_update(db: & mut Database, table_id: i32, object_id: i64, 
    version: i64, values: Vec<Value>) -> Result<Response, i32> 
{
    // Make sure that the table id is valid!
    let mut table_exists = false;
    let mut id : usize = 0;
    for i in &mut db.schema { 
        if i.t_id == table_id {
            table_exists = true;

            // Make sure the correct number of cols was supplied
            if i.t_cols.len() != values.len(){
                return Err(Response::BAD_ROW);
            }
            
            break;
        }
        id = id + 1;
    }

    // Check if the table exists
    if !table_exists {
        return Err(Response::BAD_TABLE);
    }

    // Make sure that ever value has the correct column type
    let mut col_id : usize = 0;
    for i in &values{
        match i {
            Value::Null => if !check_type(0, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(i) => if !check_type(1, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Float(i) => if !check_type(2, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Text(i) => if !check_type(3, db.schema[id].t_cols[col_id].c_type) {
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(i) => 
                if !check_type(4, db.schema[id].t_cols[col_id].c_type) {
                    return Err(Response::BAD_VALUE);
                }
                // Make sure the foreign key exists...
                else{
                    // First get the foreign table id...
                    let f_key = (db.schema[id].t_cols[col_id].c_ref - 1) as usize;

                    let search = Row::new( &i);
                    // Search for the key and return the version and values
                    match db.tables[f_key].get(&search) {
                        Some(x) => (),
                        _ => return Err(Response::BAD_FOREIGN),
                    }
                },
        }
        col_id = col_id + 1;
    }

    // Find the correct row...
    let search = Row::new(&object_id);

    match db.tables[id].get_mut(&search) {
        Some(x) => {
            // Make sure that the version number matches...
            if version != 0 && x.version != version {
                return Err(Response::TXN_ABORT);
            }
            else{
                // Now update each field and update the version number
                for i in 0..x.info.len() {
                    x.info[i] = match &values[i] {
                        Value::Null => Value::Null,
                        Value::Integer(val) => Value::Integer(*val),
                        Value::Float(val) => Value::Float(*val),
                        Value::Text(val) => Value::Text(val.to_string()),
                        Value::Foreign(val) => Value::Foreign(*val),
                    }
                }

                x.version = x.version + 1;
                
                return Ok(Response::Update(x.version));
            }
        },
        _ => {
            return Err(Response::NOT_FOUND);
        }
    }
}

// Harry 
fn handle_drop(db: & mut Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // Make sure that the table id is valid!
    let mut table_exists = false;
    let mut id : usize = 0;
    for i in &mut db.schema { 
        if i.t_id == table_id {
            table_exists = true;    
            break;
        }
        id = id + 1;
    }

    // Check if the table exists
    if !table_exists {
        return Err(Response::BAD_TABLE);
    }
    
    // // Find the correct row...
    let mut row_to_be_dropped : Vec<(i64, i64)> = vec![];
    
    for table in &db.schema {
        for col in &table.t_cols {
            if col.c_ref == table_id {
                let query_result = handle_query(db, table.t_id, col.c_id, OP_EQ, Value::Foreign(object_id));

                match query_result {
                    Result::Ok(x) => {
                        match x {
                            Response::Query(y) => {
                                for i in y {
                                    row_to_be_dropped.push((i, table.t_id as i64));
                                }
                            },
                            _ => (),
                        }
                    },
                    _ => (),
                }

            }
        }
    }

    let search = Row::new(&object_id);

    for foreign_drop in row_to_be_dropped{
        
        handle_drop(db, foreign_drop.1 as i32, foreign_drop.0);
        
    }

    db.tables[id].remove(&search);

    return Ok(Response::Drop);

}

// Shayshu
fn handle_get(db: & Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // Check if the table id is valid...
    let mut table_exists = false;
    let mut id : usize = 0;

    for i in &db.schema { 
        if i.t_id == table_id {
            table_exists = true;
            break;
        }
        id = id + 1;
    }

    if !table_exists {
        return Err(Response::BAD_TABLE);
    }

    //Construct the key 
    let search = Row::new(&object_id);

    // Search for the key and return the version and values
    match db.tables[id].get(&search) {
        Some(x) => return Ok(Response::Get(x.version, &x.info)),
        _ => return Err(Response::NOT_FOUND)
    }
}

// Harry 
fn handle_query(db: & Database, table_id: i32, column_id: i32,
    operator: i32, other: Value) 
    -> Result<Response, i32>
{
    //test if table exist
    let mut table_exists = false;
    let mut t_id:usize = 0;
    for i in &db.schema {
        if i.t_id == table_id {
            table_exists = true;
            break;
        }
        t_id = t_id +1;
    }
    if !table_exists {
        return Err(Response::BAD_TABLE);
    }

    //test if column exist
    let mut col_exists = false;
    let mut c_id:usize = 0;
    //let mut id
    for i in &db.schema[t_id].t_cols {
        if i.c_id == column_id {
            col_exists = true;
            match &other {
                Value::Null => {},
                Value::Integer(x) => {
                    if !check_type(1, i.c_type) {
                        return Err(Response::BAD_QUERY);
                    }
                },
                Value::Float(x) => {
                    if !check_type(2, i.c_type) {
                        return Err(Response::BAD_QUERY);
                    }
                },
                Value::Text(x) => {
                    if !check_type(3, i.c_type) {
                        return Err(Response::BAD_QUERY);
                    }
                },
                Value::Foreign(x) => {
                    if !check_type(4, i.c_type) {
                        return Err(Response::BAD_QUERY);
                    }
                },
            }
            break;
        }
        c_id = c_id+1;
    }
    if !col_exists && column_id != 0 {
        return Err(Response::BAD_QUERY);
    }

    //check if valid with column type
    match operator {
        OP_AL => {
            //for AL column id must be 0
            if column_id != 0 {
                return Err(Response::BAD_QUERY);
            }
        },
        OP_EQ | OP_NE=> {},
        OP_LT..=OP_GE => {
            //id column, only allows EQ and NE
            if column_id == 0 {
                return Err(Response::BAD_QUERY);
            }
            //if foreign, only allows EQ and NE
            if *&db.schema[t_id].t_cols[c_id].c_type == Value::FOREIGN {
                return Err(Response::BAD_QUERY);
            }
        },
        //all other OP codes are invalid
        _ => {
            return Err(Response::BAD_QUERY);
        },
    }

    let mut ids:Vec<i64> = Vec::new();
    for (row, rowinfo) in &db.tables[t_id] {
        if operator == OP_AL {
            ids.push(row.row_id);
        }
        else if operator == OP_EQ {
            if other == rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
        else if operator == OP_NE {
            if other != rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
        else if operator == OP_LT {
            if other > rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
        else if operator == OP_GT {
            if other < rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
        else if operator == OP_LE {
            if other >= rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
        else if operator == OP_GE {
            if other <= rowinfo.info[(column_id-1) as usize] {
                ids.push(row.row_id);
            }
        }
    }
    Ok(Response::Query(ids))
}

