# Database Management Systems Implementation

Welcome to the **Database Management Systems Implementation** project. The goal was to develop a mini relational DBMS with Python that can handle SQL-like operations, providing features similar to a real database management system.

## 💡 **Overview**

This project implements a mini relational DBMS with server and client components. It supports the following features:

- Execution of basic SQL-like commands:
  - `CREATE DATABASE`, `CREATE TABLE`
  - `INSERT`, `DELETE`, `SELECT`
  - `DROP DATABASE`, `DROP TABLE`
- Implementation of indexing mechanisms (B+ trees) for efficient query processing.
- Support for **JOIN** operations and other complex SQL clauses like **GROUP BY** and **HAVING**.

## 🚀 **Features**

### Lab 1: Basic Database Setup
- **CREATE DATABASE** and **CREATE TABLE** functionalities.
- Structure and constraints stored in metadata files.

### Lab 2: Data Manipulation (Insert/Delete)
- Implemented **INSERT** and **DELETE** operations.
- Data is stored as key-value pairs using the B+ tree structure.

### Lab 3: Indexing
- Creation of indexes using B+ trees for both unique and non-unique keys.
- Indexed attributes include primary keys and foreign keys.

### Lab 4: SELECT Query Implementation
- **SELECT** statement with filtering conditions.
- **DISTINCT** clause for removing duplicates from query results.

### Lab 5: Join Operations
- Implemented **INNER JOIN** operations using different join algorithms:
  - Indexed Nested Loop Join.
  - Hash Join.

### Lab 6: Group By and Having Clauses
- Added support for **GROUP BY** and **HAVING** clauses.
- Indexes were used to optimize these operations.
