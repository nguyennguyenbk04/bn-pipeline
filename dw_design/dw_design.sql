CREATE DATABASE store_dw;
USE store_dw;


-- Date Dimension
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY, -- Format: YYYYMMDD
    Date DATE,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    DayOfWeek INT
);

-- Product Dimension
CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    CategoryID INT,
    SellerID INT
);

-- Category Dimension
CREATE TABLE DimCategory (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(50)
);

-- Seller Dimension
CREATE TABLE DimSeller (
    SellerID INT PRIMARY KEY,
    SellerName VARCHAR(100)
);

-- Customer Dimension
CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100)
);

-- Order Status Dimension
CREATE TABLE DimOrderStatus (
    StatusID INT PRIMARY KEY,
    StatusName VARCHAR(50)
);

-- Reason Dimension (for returns/cancellations)
CREATE TABLE DimReason (
    ReasonID INT PRIMARY KEY,
    ReasonType VARCHAR(20),
    ReasonDescription TEXT
);

-- Fact Table: Sales (Order Items)
CREATE TABLE FactSales (
    OrderItemID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    SellerID INT,
    CustomerID INT,
    CategoryID INT,
    OrderDateKey INT, -- FK to DimDate
    StatusID INT,
    Quantity INT,
    CurrentPrice DECIMAL(18,2),
    UnitCost DECIMAL(18,2),      -- Added column
    Profit DECIMAL(18,2),        -- Added column
    Revenue DECIMAL(18,2),
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
    FOREIGN KEY (SellerID) REFERENCES DimSeller(SellerID),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (CategoryID) REFERENCES DimCategory(CategoryID),
    FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (StatusID) REFERENCES DimOrderStatus(StatusID)
);

-- Fact Table: Returns & Cancellations
CREATE TABLE FactOrderReason (
    OrderItemID INT PRIMARY KEY,
    ReasonID INT,
    OrderID INT,
    SellerID INT,
    OrderDateKey INT, -- FK to DimDate
    StatusID INT,
    FOREIGN KEY (ReasonID) REFERENCES DimReason(ReasonID),
    FOREIGN KEY (SellerID) REFERENCES DimSeller(SellerID),
    FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (StatusID) REFERENCES DimOrderStatus(StatusID)
);

