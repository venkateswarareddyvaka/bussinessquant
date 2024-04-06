const fs = require('fs');
const csv = require('csv-parser');
const express = require("express");
const mysql = require('mysql2/promise');

const app = express();
const port = 3000;

const pool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'bussinessquant',
});

async function createTableFromCSV() {
    try {
        const data = await new Promise((resolve, reject) => {
            const rows = [];
            fs.createReadStream('Sample-Data-Historic-1.csv')
                .pipe(csv())
                .on('data', row => rows.push(row))
                .on('end', () => resolve(rows))
                .on('error', reject);
        });

        const columns = Object.keys(data[0]).map(columnName => {
            const columnType = typeof data[0][columnName] === 'number' ? 'INT' : 'VARCHAR(255)';
            return `${columnName} ${columnType}`;
        }).join(', ');

        const createTableSQL = `CREATE TABLE IF NOT EXISTS bussinessquant (${columns})`;
        await pool.query(createTableSQL);

        const values = data.map(row => Object.values(row));
        const insertDataSQL = `INSERT INTO bussinessquant (${Object.keys(data[0]).join(', ')}) VALUES ?`;
        await pool.query(insertDataSQL, [values]);
        
        console.log(`${values.length} rows inserted.`);
    } catch (error) {
        console.error('Error:', error);
    }
}

async function queryDatabase(sqlQuery, params) {
    try {
        const [rows] = await pool.query(sqlQuery, params);
        return rows;
    } catch (error) {
        throw error;
    }
}

app.get('/', (req, res) => {
    res.send("Hello World");
});

app.get("/ticker=:symbol&column=:column&period=:period", async (req, res) => {
    try {
        const symbol = req.params.symbol.toUpperCase();
        const columns = req.params.column.split(',').map(col => col.trim());
        const periodInYears = parseInt(req.params.period?.slice(0, 1) || '0');

        const sqlQuery = `SELECT ${columns.length > 0 ? columns.join(',') : '*'} FROM bussinessquant WHERE ticker = ? AND YEAR(date) >= YEAR(CURDATE()) - ?`;
        const rows = await queryDatabase(sqlQuery, [symbol, periodInYears]);

        if (rows.length > 0) {
            res.json(rows);
        } else {
            res.status(404).send('No data found for the provided symbol and period.');
        }
    } catch (error) {
        console.error('Error:', error);
        res.status(500).send('Internal Server Error');
    }
});

async function initializeServer() {
    try {
        await createTableFromCSV();
        app.listen(port, () => {
            console.log(`Server is listening at http://localhost:${port}`);
        });
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}

initializeServer();