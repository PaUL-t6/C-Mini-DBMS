# Mini-DBMS Feature Demonstration Scripts

This document provides a step-by-step guide to demonstrating the core features of the Mini-DBMS project. Copy and paste these scripts into the query editor in the frontend.

---

## 1. Basic Table Operations
Create a table and populate it with data to demonstrate the **Storage Engine** and **Schema-Agnostic** capabilities.

```sql
-- Create a students table
CREATE TABLE students (id int, name varchar, age int);

-- Insert sample records
INSERT INTO students VALUES (1, 'Alice', 20);
INSERT INTO students VALUES (2, 'Bob', 22);
INSERT INTO students VALUES (3, 'Charlie', 21);

-- Select all records
SELECT * FROM students;
```

---

## 2. Filtering & Aggregation
Demonstrate the **Query Executor**'s ability to filter data and perform calculations.

```sql
-- Filter by ID
SELECT * FROM students WHERE id = 1;

-- Filter by Age
SELECT * FROM students WHERE age > 20;

-- Count total records
SELECT COUNT(*) FROM students;
```

---

## 3. Relational JOINs
Demonstrate the **Relational Algebra** engine by joining two different tables.

```sql
-- 1. Create a courses table
CREATE TABLE courses (cid int, student_id int, course_name varchar);

-- 2. Insert course records
INSERT INTO courses VALUES (101, 1, 'Computer Science');
INSERT INTO courses VALUES (102, 2, 'Mathematics');
INSERT INTO courses VALUES (103, 1, 'Physics');

-- 3. Perform a JOIN to see which student is taking which course
SELECT * FROM students JOIN courses ON students.id = courses.student_id;
```

---

## 4. Advanced Querying (Sorting & Grouping)
Demonstrate the **Optimizer** and **Result Processing** features.

```sql
-- Sort students by age descending
SELECT * FROM students ORDER BY age DESC;

-- Group by age to count students of the same age
SELECT * FROM students GROUP BY age;

-- Use HAVING to filter groups
SELECT * FROM students GROUP BY age HAVING age > 20;
```

---

## 5. Execution Analysis (EXPLAIN)
Demonstrate the **Query Plan Visualization**. This shows how the database intends to execute your query.

```sql
-- Explain a simple select
EXPLAIN SELECT * FROM students;

-- Explain a complex join
EXPLAIN SELECT * FROM students JOIN courses ON students.id = courses.student_id;
```

---

## 6. Data Modification (Update & Delete)
Demonstrate the **Data Persistence** and modification capabilities.

```sql
-- Update a student's age
UPDATE students SET age = 23 WHERE id = 2;

-- Verify the update
SELECT * FROM students WHERE id = 2;

-- Delete a record
DELETE FROM students WHERE id = 3;

-- Verify the deletion
SELECT * FROM students;
```

---

## 7. Performance & Optimization (Index Verification)
Use these queries to demonstrate how the **Optimizer** chooses different strategies based on data distribution.

### A. Hash Table Strategy (O(1))
Triggered by searching for a **unique** primary key.
```sql
-- Ensure unique data
CREATE TABLE perf_test (id int, val varchar);
INSERT INTO perf_test VALUES (1, 'A');
INSERT INTO perf_test VALUES (2, 'B');

-- Verify Hash Index use
EXPLAIN SELECT * FROM perf_test WHERE id = 1;
```

### B. B+ Tree Strategy (O(log N))
Triggered by searching for an indexed column with **duplicate** values.
```sql
-- Add duplicate IDs
INSERT INTO perf_test VALUES (1, 'A_Duplicate');

-- Verify B+ Tree use
EXPLAIN SELECT * FROM perf_test WHERE id = 1;
```

### C. Full Table Scan (O(N))
Triggered by searching for a **non-indexed** column.
```sql
-- Verify Table Scan
EXPLAIN SELECT * FROM perf_test WHERE val = 'B';
```

---

## 💡 Pro-Tip for Presentation
When demonstrating the **JOIN** feature, point out how the schema merges dynamically in the results table. When using **EXPLAIN**, look at the "Execution Plan" tab in the UI to show the nested-loop join or sequential scan details. Observe how the **Tree Height** and **Matches** metrics update in real-time.
