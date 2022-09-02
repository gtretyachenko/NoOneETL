# !/usr/bin/python3
# -*- coding: utf-8 -*-

import pymysql
from pymysql import Error


def create_connection(host_name, user_name, user_password, db_name=None):
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print('Connection to MySQL DB successful')
    except Error as e:
        print(f'The error {e} occurred')

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


def executemany_query(connection, query, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(query, val)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


connection = create_connection('localhost', 'root', 'suCCess15171517')

create_database_query = "CREATE DATABASE sm_app"
create_database(connection, create_database_query)

connection = create_connection('localhost', 'root', 'suCCess15171517', 'sm_app')

create_users_table = """
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT, 
  name TEXT NOT NULL, 
  age INT, 
  gender TEXT, 
  nationality TEXT, 
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
execute_query(connection, create_users_table)

create_users = """
INSERT INTO
  `users` (`name`, `age`, `gender`, `nationality`)
VALUES
  ('James', 25, 'male', 'USA'),
  ('Leila', 32, 'female', 'France'),
  ('Brigitte', 35, 'female', 'England'),
  ('Mike', 40, 'male', 'Denmark'),
  ('Elizabeth', 21, 'female', 'Canada');
"""
execute_query(connection, create_users)

create_posts_table = """
CREATE TABLE IF NOT EXISTS posts (
  id INT AUTO_INCREMENT, 
  title TEXT NOT NULL, 
  description TEXT NOT NULL, 
  user_id INT NOT NULL, 
  FOREIGN KEY fk_user_id (user_id) REFERENCES users(id), 
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

execute_query(connection, create_posts_table)

create_posts = """
INSERT INTO 
  `posts` (`title`, `description`, `user_id`)
VALUES
  ('Happy', 'I am feeling very happy today', 1),
  ('Hot Weather', 'The weather is very hot today', 2),
  ('Help', 'I need some help with my work', 2),
  ('Great News', 'I am getting married', 1),
  ('Interesting Game', 'It was a fantastic game of tennis', 5),
  ('Party', 'Anyone up for a late-night party today?', 3);
"""

execute_query(connection, create_posts)

create_comments_table = """
CREATE TABLE IF NOT EXISTS comments (
  id INT AUTO_INCREMENT, 
  text TEXT NOT NULL, 
  user_id INT NOT NULL, 
  post_id INT NOT NULL,
  PRIMARY KEY (id), 
  FOREIGN KEY (user_id) REFERENCES users (id), FOREIGN KEY (post_id) REFERENCES posts (id)
);
"""

execute_query(connection, create_comments_table)

create_comments = """
INSERT INTO
  `comments` (`text`, `user_id`, `post_id`)
VALUES
  ('Count me in', 1, 6),
  ('What sort of help?', 5, 3),
  ('Congrats buddy', 2, 4),
  ('I was rooting for Nadal though', 4, 5),
  ('Help with your thesis?', 2, 3),
  ('Many congratulations', 5, 4);
"""

execute_query(connection, create_comments)

create_likes_table = """
CREATE TABLE IF NOT EXISTS likes (
  id INT AUTO_INCREMENT, 
  user_id INT NOT NULL, 
  post_id INT NOT NULL,
  PRIMARY KEY (id), 
  FOREIGN KEY (user_id) REFERENCES users (id), FOREIGN KEY (post_id) REFERENCES posts (id)
);
"""

execute_query(connection, create_likes_table)

sql = "INSERT INTO likes ( user_id, post_id ) VALUES ( %s, %s )"
val = [(4, 5), (3, 4)]

executemany_query(connection, sql, val)


update_post_description = """
UPDATE
  posts
SET
  description = "The weather has become pleasant now"
WHERE
  id = 2
"""

execute_query(connection,  update_post_description)


select_users = "SELECT * FROM users"
users = execute_read_query(connection, select_users)

for user in users:
    print(user)

