# Mini-DBMS: A Schema-Agnostic Relational Database Engine

Mini-DBMS is a high-performance, three-tier database management system featuring a C-based storage engine, a Node.js middleware layer, and a React frontend. It supports dynamic custom tables, advanced indexing, and relational SQL operations.

## 🚀 Features

- **Core Storage Engine (C)**: Low-level data management with row-level serialization.
- **Advanced Indexing**: Supports both **B+ Tree** and **Hash Indexing** for optimized data retrieval.
- **SQL Parser & Optimizer**: Handles complex queries including `SELECT`, `INSERT`, and relational `JOIN` operations.
- **Relational Joins**: Implementation of Nested-Loop Joins for multi-table queries.
- **Schema-Agnostic Architecture**: Support for dynamic table creation without predefined fixed schemas.
- **Full-Stack Interface**:
  - **React Frontend**: Interactive query editor and data visualization.
  - **Node.js Middleware**: Robust process management and API routing.

## 🛠 Tech Stack

- **Backend**: C (MinGW/POSIX compatible)
- **Middleware**: Node.js, Express
- **Frontend**: React, CSS3
- **Tools**: Makefile, Git

## 📂 Project Structure

```text
├── backend/            # C Source code (Storage engine, Parser, Optimizer)
│   ├── include/        # Header files
│   ├── src/            # Implementation files
│   └── Makefile        # Build system
├── middleware/         # Node.js API and process management
├── frontend/           # React user interface
└── screenshots/        # UI/UX demonstrations
```

## 🚦 Getting Started

### Prerequisites
- GCC Compiler (MinGW for Windows)
- Node.js & npm

### Installation & Execution

1. **Compile the Backend**:
   ```bash
   cd backend
   make
   ```

2. **Start the Middleware**:
   ```bash
   cd ../middleware
   npm install
   npm start
   ```

3. **Launch the Frontend**:
   ```bash
   cd ../frontend
   npm install
   npm start
   ```

## 📝 License
This project is licensed under the MIT License.
