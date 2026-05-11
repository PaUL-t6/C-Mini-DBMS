



CREATE TABLE products (id int, name varchar, price int, cat_id int)


CREATE TABLE categories (id int, title varchar)

CREATE TABLE users (id int, username varchar, age int)



The parser generically handles any number of comma-separated values for any table.

INSERT INTO categories VALUES (1, Electronics)
INSERT INTO categories VALUES (2, Furniture)


INSERT INTO products VALUES (101, Laptop, 1200, 1)
INSERT INTO products VALUES (102, Phone, 800, 1)
INSERT INTO products VALUES (103, Chair, 150, 2)
INSERT INTO products VALUES (104, Desk, 450, 2)


INSERT INTO users VALUES (1, Alice, 25)
INSERT INTO users VALUES (2, Bob, 30)
INSERT INTO users VALUES (3, Charlie, 22)





SELECT name, price FROM products


SELECT * FROM users WHERE username = Bob


SELECT * FROM products WHERE price > 400

SELECT * FROM users WHERE age < 25



EXPLAIN SELECT * FROM users WHERE id = 1


EXPLAIN SELECT * FROM users WHERE age = 30





SELECT * FROM products ORDER BY price DESC


SELECT * FROM users ORDER BY username ASC



SELECT * FROM products JOIN categories ON products.cat_id = categories.id



UPDATE users SET age = 31 WHERE id = 2


SELECT * FROM users WHERE id = 2


DELETE FROM products WHERE id = 104


SELECT * FROM products



SELECT COUNT(*) FROM users

SELECT * FROM products GROUP BY cat_id HAVING COUNT(*) > 1




