CREATE DATABASE IF NOT EXISTS online_course_tracker;
USE online_course_tracker;

CREATE TABLE students (student_id INT PRIMARY KEY AUTO_INCREMENT, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, email VARCHAR(100) UNIQUE NOT NULL, join_date DATE);

CREATE TABLE courses (course_id INT PRIMARY KEY AUTO_INCREMENT, course_name VARCHAR(100) NOT NULL, instructor VARCHAR(100), duration_weeks INT);

CREATE TABLE enrollments (enrollment_id INT PRIMARY KEY AUTO_INCREMENT, student_id INT, course_id INT, enrollment_date DATE, status VARCHAR(20), FOREIGN KEY (student_id) REFERENCES students(student_id), FOREIGN KEY (course_id) REFERENCES courses(course_id));

CREATE TABLE progress (progress_id INT PRIMARY KEY AUTO_INCREMENT, enrollment_id INT, completion_percentage DECIMAL(5,2), last_updated DATE, FOREIGN KEY (enrollment_id) REFERENCES enrollments(enrollment_id));

INSERT INTO students (first_name,last_name,email,join_date) VALUES ('Rahul','Sharma','rahul@example.com
','2026-05-01'),('Priya','Reddy','priya@example.com
','2026-05-02'),('Arjun','Kumar','arjun@example.com
','2026-05-03'),('Sneha','Patel','sneha@example.com
','2026-05-04'),('Aravind','Rao','aravind@example.com
','2026-05-05');

INSERT INTO courses (course_name,instructor,duration_weeks) VALUES ('Python Basics','John Smith',6),('Data Engineering','Sara Lee',8),('SQL Mastery','David Brown',4);

INSERT INTO enrollments (student_id,course_id,enrollment_date,status) VALUES (1,1,'2026-05-04','Active'),(2,2,'2026-05-05','Active'),(3,3,'2026-05-06','Dropped'),(4,1,'2026-05-07','Completed'),(5,2,'2026-05-08','Active');

INSERT INTO progress (enrollment_id,completion_percentage,last_updated) VALUES (1,75.00,'2026-05-10'),(2,45.00,'2026-05-10'),(3,20.00,'2026-05-10'),(4,100.00,'2026-05-10'),(5,65.00,'2026-05-10');

INSERT INTO enrollments (student_id,course_id,enrollment_date,status) VALUES (1,2,'2026-05-11','Active');

SELECT * FROM students;
SELECT * FROM courses;
SELECT * FROM enrollments;
SELECT * FROM progress;

UPDATE progress SET completion_percentage=85.00 WHERE enrollment_id=1;

DELETE FROM enrollments WHERE enrollment_id=3;

CREATE INDEX idx_student_id ON enrollments(student_id);
CREATE INDEX idx_course_id ON enrollments(course_id);

DELIMITER //

CREATE PROCEDURE GetCompletionPercentage(IN studentId INT)
BEGIN
SELECT s.student_id, CONCAT(s.first_name,' ',s.last_name) AS student_name, c.course_name, p.completion_percentage FROM students s JOIN enrollments e ON s.student_id=e.student_id JOIN courses c ON e.course_id=c.course_id JOIN progress p ON e.enrollment_id=p.enrollment_id WHERE s.student_id=studentId;
END //

DELIMITER ;

CALL GetCompletionPercentage(1);

SELECT c.course_name, COUNT(e.enrollment_id) AS total_students, AVG(p.completion_percentage) AS avg_completion FROM courses c JOIN enrollments e ON c.course_id=e.course_id JOIN progress p ON e.enrollment_id=p.enrollment_id GROUP BY c.course_name;

SELECT s.first_name, s.last_name, c.course_name, e.status FROM students s JOIN enrollments e ON s.student_id=e.student_id JOIN courses c ON e.course_id=c.course_id WHERE e.status='Dropped';

SELECT c.course_name, COUNT(e.enrollment_id) AS enrollment_count FROM courses c JOIN enrollments e ON c.course_id=e.course_id GROUP BY c.course_name ORDER BY enrollment_count DESC;

SELECT s.student_id, CONCAT(s.first_name,' ',s.last_name) AS student_name, c.course_id, c.course_name, e.enrollment_date, p.completion_percentage AS completion FROM students s JOIN enrollments e ON s.student_id=e.student_id JOIN courses c ON e.course_id=c.course_id JOIN progress p ON e.enrollment_id=p.enrollment_id;