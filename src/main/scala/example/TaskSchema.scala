package example

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object TaskSchema {
  // CREATE SCHEMAS (FOR DOCUMENTATION PURPOSES ONLY)

  // The model of the example:
  // 1. We have a catalogue of products.
  // 2. Users place orders buying products.
  // 3. Users pay for the orders by making payments.
  // 4. Products belong to categories.

  val categorySchema: StructType = StructType(Array(
    StructField("CategoryId", LongType), // Category ID
    StructField("Name", StringType),     // Name of the category
    StructField("Code", StringType),     // Code of the category
    StructField("Country", StringType)   // Where this category is from
  ))

  val productSchema: StructType = StructType(Array(
    StructField("ProductId", LongType),           // Product ID
    StructField("Name", StringType),              // Name of the product
    StructField("Country", StringType),           // Where it is available
    StructField("Price", DoubleType),             // At what price
    StructField("Weight", DoubleType),            // How heavy it is
    StructField("MarketEntranceDate", StringType) // When it entered the market
  ))

  val orderSchema: StructType = StructType(Array(
    StructField("OrderId", LongType),               // Order ID
    StructField("PaymentId", LongType),             // Payment ID
    StructField("UserId", LongType),                // User ID
    StructField("ProductId", LongType),             // Product ID
    StructField("Country", StringType),             // Where the order came from
    StructField("Count", LongType),                 // How many instances
    StructField("TotalValue", DoubleType),          // Total value of the order
    StructField("TotalWeight", DoubleType),         // Total weight of the order
    StructField("OrderGenerationDate", StringType), // When the order was generated
    StructField("OrderCompletionDate", StringType), // When the order was completed
    StructField("Status", StringType)               // COMPLETED or NOT COMPLETED
  ))

  val userSchema: StructType = StructType(Array(
    StructField("UserId", LongType),        // User ID
    StructField("FirstName", StringType),   // First name of the user
    StructField("LastName", StringType),    // Last name of the user
    StructField("Country", StringType),     // Where she / he comes from
    StructField("City", StringType),        // Where she / he lives
    StructField("PostalCode", StringType),  // Her / his postal code
    StructField("Address", StringType)      // Her / his full address
  ))

  val paymentSchema: StructType = StructType(Array(
    StructField("PaymentId", LongType),               // Payment ID
    StructField("Country", StringType),               // Where the payment belongs to
    StructField("TotalValue", DoubleType),            // Total amount to pay
    StructField("PaymentDeadline", StringType),       // When it needs to be paid
    StructField("PaymentCompletionDate", StringType), // When it was paid
    StructField("PaymentStatus", StringType)          // COMPLETED or NOT COMPLETED
  ))
}
