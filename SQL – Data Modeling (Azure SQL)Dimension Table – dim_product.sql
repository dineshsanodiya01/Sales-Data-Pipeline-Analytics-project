CREATE TABLE FactSales (
    SalesID INT IDENTITY PRIMARY KEY,
    ProductID INT,
    CustomerID INT,
    RegionID INT,
    TimeID INT,
    SalesAmount DECIMAL(18,2),
    Quantity INT
);

Dimension Table – dim_product.sql

CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    SubCategory VARCHAR(50)
);
