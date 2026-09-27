CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    balance NUMERIC(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email, balance)
VALUES
    ('Ismail', 'ish@example.com', 250.00),
    ('David', 'david@example.com', 100.00),
    ('Zeki', 'zeki@example.com', 500.00);
