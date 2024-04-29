using System.Data.SqlClient;
using System.Data;
using static System.Console;

namespace TechnicalTask_Mindbox_3
{
    internal class Program
    {
        private const string DatabaseName = "TaskProducts";
        private const string TableProducts = "Products";
        private const string TableCategories = "Categories";
        private const string TableProductCategories = "ProductCategories";

        private static void Main(string[] args)
        {
            var connectionString = @"Server=(localdb)\mssqllocaldb;Trusted_Connection=True;";
            InitializeDataBase(connectionString);

            var table = new DataTable();
            GetProductsAndCategories(connectionString, table);

            // Вывод всех продуктов и их категорий
            foreach (DataRow row in table.Rows)
            {
                foreach (var item in row.ItemArray)
                {
                    Write(item + "\t");
                }
                WriteLine();
            }

            ReadLine();
        }

        private static void GetProductsAndCategories(string connectionString, DataTable table)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand($@"USE {DatabaseName}
                                                       SELECT p.ProductName AS ProductName, ISNULL(c.CategoryName, 'No Category') AS CategoryName
                                                       FROM [Products] p
                                                       LEFT JOIN [ProductCategories] pc ON p.ProductId = pc.ProductId
                                                       LEFT JOIN [Categories] c ON pc.CategoryId = c.CategoryId;",
                                                       connection))
                {
                    var adapter = new SqlDataAdapter(command);
                    adapter.Fill(table);
                }
            }
        }

        private static void InitializeDataBase(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // Создаём базу данных
                ExecuteCommand($@"IF EXISTS (SELECT * FROM sys.databases WHERE name = '{DatabaseName}')
                                  BEGIN
                                      DROP DATABASE {DatabaseName};
                                  END
                                  CREATE DATABASE {DatabaseName}",
                                  connection);

                // Создаём таблицы
                ExecuteCommand($@"USE {DatabaseName}
                                  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableProducts}')
                                  BEGIN
                                      CREATE TABLE {TableProducts}
                                      (
                                          [ProductId] INT PRIMARY KEY IDENTITY,
                                          [ProductName] NVARCHAR(100) NOT NULL
                                      );
                                  END

                                  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableCategories}')
                                  BEGIN
                                      CREATE TABLE {TableCategories} 
                                      (
                                          [CategoryId] int identity primary key,
                                          [CategoryName] nvarchar(100) NOT NULL
                                      );
                                  END

                                  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableProductCategories}')
                                  BEGIN
                                      CREATE TABLE {TableProductCategories} 
                                      (
                                          ProductId int,
                                          CategoryId int NULL,
                                          FOREIGN KEY (ProductId) REFERENCES Products(ProductId),
                                          FOREIGN KEY (CategoryId) REFERENCES Categories(CategoryId)
                                      );
                                  END",
                                  connection);

                // Заполнение таблиц
                ExecuteCommand($@"INSERT INTO Products ([ProductName]) VALUES ('Product_1');
                                  INSERT INTO Products ([ProductName]) VALUES ('Product_2');
                                  INSERT INTO Products ([ProductName]) VALUES ('Product_3');
                                  INSERT INTO Products ([ProductName]) VALUES ('Product_4');
                                  INSERT INTO Products ([ProductName]) VALUES ('Product_5');
                                  
                                  INSERT INTO Categories ([CategoryName]) VALUES ('Category_1');
                                  INSERT INTO Categories ([CategoryName]) VALUES ('Category_2');
                                  INSERT INTO Categories ([CategoryName]) VALUES ('Category_3');
                                  INSERT INTO Categories ([CategoryName]) VALUES ('Category_4');
                                  INSERT INTO Categories ([CategoryName]) VALUES ('Category_5');
                                  
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (1, 1);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (1, 2);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (1, 3);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (2, 4);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (2, 5);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (3, 2);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (3, 4);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (4, 1);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (4, 5);
                                  INSERT INTO ProductCategories (ProductId, CategoryId) VALUES (5, null);",
                                  connection);
            }
        }

        private static void ExecuteCommand(string commandText, SqlConnection connection)
        {
            using (var command = new SqlCommand(commandText, connection))
            {
                command.ExecuteNonQuery();
            }
        }
    }
}
