### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    with open(data_filename,'r') as f:
        header=None
        regions=[]
        for line in f:
            line=line.strip()
            if not line:
                continue
            if not header:
                header=line.split("\t")
            else:
                data=line.split('\t')
                r=data[4]
                if r not in regions:
                    regions.append(r)
        regions=sorted(regions)
    conn=create_connection(normalized_database_filename)
    create_table_sql='''
    CREATE TABLE IF NOT EXISTS Region (
        RegionID INTEGER NOT NULL PRIMARY KEY,
        Region TEXT NOT NULL
    )
    '''
    create_table(conn,create_table_sql,drop_table_name='Region')
    with conn:
        for i, val in enumerate(regions):
            insert_sql=f"INSERT INTO Region (RegionID, Region) VALUES ({i+1}, '{val}')"
            execute_sql_statement(insert_sql,conn)
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    sql_statement="SELECT RegionID, Region FROM Region"
    rows=execute_sql_statement(sql_statement,conn)
    d={}
    for row in rows:
        d[row[1]]=row[0]
    return d
    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    d=step2_create_region_to_regionid_dictionary(normalized_database_filename)
    conn=create_connection(normalized_database_filename)
    create_table_sql="""
            CREATE TABLE IF NOT EXISTS Country (
                CountryID INTEGER NOT NULL PRIMARY KEY,
                Country TEXT NOT NULL,RegionID INTEGER NOT NULL,
                FOREIGN KEY (RegionID) REFERENCES Region (RegionID)
            );
        """
    create_table(conn, create_table_sql,"Country")
    dic_countries={}
    with open(data_filename,'r') as f:
        header=None
        regions=[]
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not header:
                header=line.split("\t")
            else:
                data=line.split('\t')
                country=data[3]
                region=data[4]
                if country not in dic_countries:
                    dic_countries[country] = region
    country_data=[]
    for c, r in sorted(dic_countries.items()):
        r_id=d[r]
        country_data.append((c,r_id))
    with conn:
        sql="INSERT INTO Country (Country, RegionID) VALUES (?, ?)"
        cur=conn.cursor()
        cur.executemany(sql,country_data) 
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    ### BEGIN SOLUTION
    conn = sqlite3.connect(normalized_database_filename)
    sql_statement="SELECT CountryID, Country FROM Country"
    rows = execute_sql_statement(sql_statement,conn)
    d={}
    for row in rows:
        c_id,country=row
        d[country]=c_id
    return d
    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    country_dict = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    conn = create_connection(normalized_database_filename)
    create_table_sql = """
            CREATE TABLE IF NOT EXISTS Customer (
                CustomerID INTEGER NOT NULL PRIMARY KEY,
                FirstName TEXT NOT NULL,LastName TEXT NOT NULL,
                Address TEXT NOT NULL,City TEXT NOT NULL,
                CountryID INTEGER NOT NULL,FOREIGN KEY (CountryID) REFERENCES Country (CountryID)
            );
        """
    create_table(conn,create_table_sql,"Customer")
    cus_data=[]
    with open(data_filename, 'r') as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not header:
                header=line.split("\t")
            else:
                data=line.split("\t")
                name=data[0].split()
                if len(name)==2:
                    firstname,lastname=name
                else:
                    firstname=name[0]
                    lastname=" ".join(name[1:])
                address=data[1]
                city=data[2]
                country=data[3]
                country_id=country_dict[country]
                cus_data.append((firstname, lastname, address, city, country_id))
    cus_data.sort(key=lambda x: (x[0], x[1]))
    print(cus_data)
    with conn:
        sql="INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES (?, ?, ?, ?, ?)"
        cur=conn.cursor()
        cur.executemany(sql,cus_data)
    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    ### BEGIN SOLUTION
    conn = sqlite3.connect(normalized_database_filename)
    sql_statement="SELECT CustomerID, FirstName, LastName FROM Customer"
    rows = execute_sql_statement(sql_statement, conn)
    d={}
    for row in rows:
        full_name= row[1] + ' ' + row[2]
        d[full_name]=row[0]
    return d
    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS ProductCategory (
            ProductCategoryID INTEGER NOT NULL PRIMARY KEY,ProductCategory TEXT NOT NULL,ProductCategoryDescription TEXT NOT NULL
        );
    """
    create_table(conn,create_table_sql,"ProductCategory")

    cts=[]
    dts=[]
    with open(data_filename, 'r') as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not header:
                header=line.split("\t")
            else:
                data=line.split("\t")
                category=data[6]
                description=data[7]

                for c in category.split(';'):
                    if c not in cts:
                        cts.append(c)

                for d in description.split(';'):
                    if d not in dts:
                        dts.append(d)

    category_data = []
    for i, category in enumerate((cts)):
        category_data.append((i+1, category, dts[i]))

    category_dict = {category_data[i][1]: category_data[i][2] for i in range(len(category_data))}

    res=[]
    for i,(category,description) in enumerate(sorted(category_dict.items()),1):
        res.append((i,category,description))

    with conn:
        sql = "INSERT INTO ProductCategory (ProductCategoryID, ProductCategory, ProductCategoryDescription) VALUES (?, ?, ?)"
        cur = conn.cursor()
        cur.executemany(sql, res)

    ### END SOLUTION



def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT * FROM ProductCategory"
    rows = execute_sql_statement(sql_statement, conn)
    d={}
    for row in rows:
        d[row[1]]=row[0]
    return d
    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    conn = create_connection(normalized_database_filename)
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS Product (
            ProductID INTEGER NOT NULL PRIMARY KEY,
            ProductName TEXT NOT NULL,
            ProductUnitPrice REAL NOT NULL,
            ProductCategoryID INTEGER NOT NULL,
            FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory (ProductCategoryID)
        );
    """
    create_table(conn, create_table_sql, "Product")
    d=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)

    res=[]
    products=[]
    with open(data_filename, "r") as f:
        header=None
        for line in f:
            line=line.strip()
            if not line:
                continue
            if not header:
                header = line.split("\t")
            else:
                data=line.split("\t")
                pn=data[5]
                pc=data[6]
                pup=data[8]
                for i in zip(pn.split(';'),pc.split(';'),pup.split(';')):
                    if i not in res:
                        res.append(i)
    sorted_list=sorted(res,key=lambda x: x[0])

    product_data = []
    for i,val in enumerate(sorted_list):
        pc_id = d[val[1]]
        product_data.append((i+1, val[0], float(val[2]), pc_id))

    with conn:
        sql = "INSERT INTO Product (ProductID, ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?, ?)"
        cur = conn.cursor()
        cur.executemany(sql,product_data)
    
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT ProductID, ProductName FROM Product"
    rows = execute_sql_statement(sql_statement, conn)
    d={}
    for row in rows:
        d[row[1]]=row[0]
    return d
    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS OrderDetail (
            OrderID INTEGER NOT NULL PRIMARY KEY,CustomerID INTEGER NOT NULL,
            ProductID INTEGER NOT NULL,OrderDate TEXT NOT NULL,QuantityOrdered INTEGER NOT NULL,
            FOREIGN KEY (CustomerID) REFERENCES Customer (CustomerID),FOREIGN KEY (ProductID) REFERENCES Product (ProductID)
        );
    """
    create_table(conn,create_table_sql,"OrderDetail") 
    d=step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    d1=step10_create_product_to_productid_dictionary(normalized_database_filename)
    from datetime import datetime
    order_details=[]
    with open(data_filename, 'r') as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not header:
                header=line.split("\t")
            else:
                data=line.split('\t')
                name= data[0]
                customer_id=d.get(name)
                if not customer_id:
                    continue  
                product_names=data[5].split(';')
                product_qo=[int(q) for q in data[9].split(';')]
                product_od=data[10].split(';')
                
                for i, val in enumerate(product_names):
                    product_id=d1.get(val)
                    if not product_id:
                        continue 
                    order_date_str=product_od[i]
                    order_date=datetime.strptime(order_date_str, '%Y%m%d').strftime('%Y-%m-%d')
                    quantity_ordered=product_qo[i]
                    order_details.append((None, customer_id, product_id, order_date, quantity_ordered))

    with conn:
        sql = "INSERT INTO OrderDetail (OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?, ?, ?, ?, ?)"
        cur = conn.cursor()
        cur.executemany(sql,order_details)
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    d=step6_create_customer_to_customerid_dictionary('normalized.db')
    sql_statement = """
    SELECT 
        C.FirstName || ' ' || C.LastName AS Name,
        P.ProductName,
        OD.OrderDate,
        P.ProductUnitPrice,
        OD.QuantityOrdered,
        ROUND(P.ProductUnitPrice * OD.QuantityOrdered, 2) AS Total
    FROM 
        OrderDetail OD
        JOIN Customer C ON C.CustomerID = OD.CustomerID
        JOIN Product P ON P.ProductID = OD.ProductID
    WHERE 
        C.CustomerID = {}
    """.format(d[CustomerName])
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    d=step6_create_customer_to_customerid_dictionary('normalized.db')
    customer_id=d.get(CustomerName)
    sql_statement = """
    SELECT c.FirstName || ' ' || c.LastName AS Name, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
        FROM OrderDetail od
        JOIN Customer c ON c.CustomerID = od.CustomerID
        JOIN Product p ON p.ProductID = od.ProductID
        WHERE c.CustomerID = {}
        GROUP BY Name
    """.format(customer_id)
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """
    SELECT c.FirstName || ' ' || c.LastName AS Name, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
        FROM OrderDetail od
        JOIN Customer c ON c.CustomerID = od.CustomerID
        JOIN Product p ON p.ProductID = od.ProductID
        GROUP BY od.CustomerID
        ORDER BY Total DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT r.Region, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
    FROM OrderDetail od
    JOIN Product p ON p.ProductID = od.ProductID
    JOIN Customer c ON c.CustomerID = od.CustomerID
    JOIN Country co ON co.CountryID = c.CountryID
    JOIN Region r ON r.RegionID = co.RegionID
    GROUP BY r.Region
    ORDER BY Total DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT co.Country, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal
    FROM OrderDetail od
    JOIN Product p ON p.ProductID = od.ProductID
    JOIN Customer c ON c.CustomerID = od.CustomerID
    JOIN Country co ON co.CountryID = c.CountryID
    GROUP BY co.Country
    ORDER BY CountryTotal DESC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
    WITH CountryTotal AS (
    SELECT r.Region,
           co.Country,
           ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal
    FROM OrderDetail od
    JOIN Product p ON p.ProductID = od.ProductID
    JOIN Customer c ON c.CustomerID = od.CustomerID
    JOIN Country co ON c.CountryID = co.CountryID
    JOIN Region r ON co.RegionID = r.RegionID
    GROUP BY r.Region, co.Country
    )
    SELECT CountryTotal.Region,
        CountryTotal.Country,
        CountryTotal.CountryTotal,
        RANK() OVER (PARTITION BY CountryTotal.Region ORDER BY CountryTotal.CountryTotal DESC) AS CountryRegionalRank
    FROM CountryTotal
    ORDER BY CountryTotal.Region ASC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
    WITH tbl AS (
    SELECT r.Region,co.Country,ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal,
           RANK() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * od.QuantityOrdered) DESC) AS CountryRegionalRank
    FROM OrderDetail od
    JOIN Product p ON p.ProductID = od.ProductID
    JOIN Customer c ON c.CustomerID = od.CustomerID
    JOIN Country co ON c.CountryID = co.CountryID
    JOIN Region r ON co.RegionID = r.RegionID
    GROUP BY r.Region, co.Country
    )
    SELECT Region, Country, CountryTotal, CountryRegionalRank
    FROM tbl
    WHERE CountryRegionalRank=1
    ORDER BY Region ASC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
    WITH tbl AS (
        SELECT 
            CustomerID,
            CAST(strftime('%Y', OrderDate) AS INTEGER) AS Year,
            CASE 
                WHEN CAST(strftime('%m', OrderDate) AS INTEGER)<=3 THEN 'Q1'
                WHEN CAST(strftime('%m', OrderDate) AS INTEGER)<=6 THEN 'Q2'
                WHEN CAST(strftime('%m', OrderDate) AS INTEGER)<=9 THEN 'Q3'
                ELSE 'Q4'
            END AS Quarter,
            ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) AS Total
        FROM OrderDetail
        JOIN Product ON OrderDetail.ProductID = Product.ProductID
        GROUP BY CustomerID, Year, Quarter
    )
    SELECT Quarter, Year, CustomerID, Total
    FROM tbl
	GROUP BY Quarter, Year, CustomerID
    ORDER BY  Year
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    WITH tbl AS (
    SELECT 
        CustomerID,CAST(strftime('%Y', OrderDate) AS INTEGER) AS Year,
        CASE 
            WHEN CAST(strftime('%m', OrderDate) AS INTEGER) <= 3 THEN 'Q1'
            WHEN CAST(strftime('%m', OrderDate) AS INTEGER) <= 6 THEN 'Q2'
            WHEN CAST(strftime('%m', OrderDate) AS INTEGER) <= 9 THEN 'Q3'
            ELSE 'Q4'
        END AS Quarter,
        ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) AS Total
    FROM OrderDetail
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    GROUP BY CustomerID, Year, Quarter),
    QuarterYearRank AS (
        SELECT Quarter, Year, CustomerID, Total, RANK() OVER (PARTITION BY Quarter, Year ORDER BY Total DESC) AS CustomerRank
        FROM tbl)
    SELECT 
        Quarter, Year, CustomerID, Total,CustomerRank
    FROM QuarterYearRank
    WHERE CustomerRank<=5
    ORDER BY Year ASC, Quarter ASC, CustomerRank ASC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
    WITH tbl1 AS (
    SELECT 
        CASE 
            WHEN strftime('%m', OrderDate) = '01' THEN 'January'
            WHEN strftime('%m', OrderDate) = '02' THEN 'February'
            WHEN strftime('%m', OrderDate) = '03' THEN 'March'
            WHEN strftime('%m', OrderDate) = '04' THEN 'April'
            WHEN strftime('%m', OrderDate) = '05' THEN 'May'
            WHEN strftime('%m', OrderDate) = '06' THEN 'June'
            WHEN strftime('%m', OrderDate) = '07' THEN 'July'
            WHEN strftime('%m', OrderDate) = '08' THEN 'August'
            WHEN strftime('%m', OrderDate) = '09' THEN 'September'
            WHEN strftime('%m', OrderDate) = '10' THEN 'October'
            WHEN strftime('%m', OrderDate) = '11' THEN 'November'
            WHEN strftime('%m', OrderDate) = '12' THEN 'December'
        END AS Month, 
        SUM(ROUND(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) AS TotalMonthlySales
    FROM OrderDetail
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    GROUP BY Month
    ), 
    monthly_sales_ranked AS (
        SELECT Month, TotalMonthlySales, RANK() OVER (ORDER BY TotalMonthlySales DESC) AS TotalRank
        FROM tbl1
    )
    SELECT Month, TotalMonthlySales AS Total, TotalRank AS TotalRank
    FROM monthly_sales_ranked
    ORDER BY TotalRank ASC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
    WITH tbl AS  
        (
            SELECT b.CustomerID,b.FirstName,b.LastName,c.Country,a.OrderDate,lag(a.OrderDate) OVER(PARTITION BY a.CustomerID ORDER BY a.OrderDate) AS PreviousOrderDate
            FROM OrderDetail a 
            JOIN Customer b ON a.CustomerID=b.CustomerID 
            JOIN Country c ON b.CountryID=c.CountryID
        ),
    tbl2 AS
        (
            SELECT *,(julianday(OrderDate) - julianday(PreviousOrderDate)) AS MaxDaysWithoutOrder
            FROM tbl WHERE PreviousOrderDate IS NOT NULL
        ),
    max_days_without_order AS
        (
            SELECT CustomerID,MAX(MaxDaysWithoutOrder) AS MaxDaysWithoutOrder
            FROM tbl2
            GROUP BY CustomerID
        )
    SELECT tbl2.CustomerID,tbl2.FirstName,tbl2.LastName,tbl2.Country,tbl2.OrderDate, tbl2.PreviousOrderDate,tbl2.MaxDaysWithoutOrder AS MaxDaysWithoutOrder
    FROM tbl2
        JOIN max_days_without_order ON tbl2.CustomerID = max_days_without_order.CustomerID
            AND tbl2.MaxDaysWithoutOrder = max_days_without_order.MaxDaysWithoutOrder
            AND tbl2.OrderDate = (SELECT MIN(OrderDate) FROM tbl2 t WHERE t.CustomerID = tbl2.CustomerID AND t.MaxDaysWithoutOrder = max_days_without_order.MaxDaysWithoutOrder)
    ORDER BY 
        tbl2.MaxDaysWithoutOrder DESC,
        tbl2.CustomerID DESC;


    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement