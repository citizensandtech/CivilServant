CREATE DATABASE civilservant_development;
CREATE DATABASE civilservant_test;
CREATE DATABASE civilservant_production;
CREATE USER 'civilservant'@'%' IDENTIFIED BY '';
GRANT ALL PRIVILEGES ON *.* to 'civilservant'@'%';
FLUSH PRIVILEGES;
