/** Hotel Data Analysis Program
 *
 * This program analyzes hotel booking dataset provided by a company that assists
 * international travelers in securing accommodations across various destination countries
 */

import com.github.tototoshi.csv.*

// class for reading and parsing data

class HotelDataReader {

  // class for data encapsulation

  case class HotelData(
    bookingId: String,
    dateOfBooking: String,
    time: String,
    customerId: String,
    gender: String,
    age: Int,
    originCountry: String,
    state: String,
    location: String,
    destinationCountry: String,
    destinationCity: String,
    numberOfPeople: Int,
    checkInDate: String,
    numberOfDays: Int,
    checkOutDate: String,
    rooms: Int,
    hotelName: String,
    hotelRating: Double,
    paymentMode: String,
    bankName: String,
    bookingPriceSGD: BigDecimal,
    discount: BigDecimal,
    gst: BigDecimal,
    profitMargin: BigDecimal,
  )

  // function for reading csv
  def readData(file: String): List[HotelData] = {
    val path: String = getClass.getResource(file).toURI.getPath
    val reader = CSVReader.open(path)
    val data = reader.allWithHeaders().map(parseRow)
    reader.close()
    data
  }

  // function to parse single row
  private def parseRow(row: Map[String, String]): HotelData = {
    HotelData(
      row("Booking ID"),
      row("Date of Booking"),
      row("Time"),
      row("Customer ID"),
      row("Gender"),
      row("Age").toInt,
      row("Origin Country"),
      row("State"),
      row("Location"),
      row("Destination Country"),
      row("Destination City"),
      row("No. Of People").toInt,
      row("Check-in date"),
      row("No of Days").toInt,
      row("Check-Out Date"),
      row("Rooms").toInt,
      row("Hotel Name"),
      row("Hotel Rating").toDouble,
      row("Payment Mode"),
      row("Bank Name"),
      row("Booking Price[SGD]").toDouble,
      row("Discount").dropRight(1).toDouble, // Remove "%"
      row("GST").toDouble,
      row("Profit Margin").toDouble,
    )
  }

}

// main program

@main
def main(): Unit = {

  // read data

  // enter file name to allow for usage of the same function for
  // dataset with different names
  val data = HotelDataReader().readData("Hotel_Dataset.csv")

  // questions

  def question1(): Unit = {
    // which country has the highest number of bookings in the dataset?

    // Step 1: Group all bookings by the origin country
    // This creates a Map[String, List[HotelData]] where the key is the country
    val grouped = data.groupBy(_.originCountry)

    // Step 2: Count the number of bookings per country
    // `mapValues(_.size)` transforms each list of bookings into its length
    val counts = grouped.mapValues(_.size)

    // Step 3: Find the country with the maximum number of bookings
    // `maxBy(_._2)` compares the counts (second element of the tuple) and returns the highest
    val (topCountry, topCount) = counts.maxBy(_._2)

    // Step 4: Print the result
    println(s"Country with the highest number of bookings:")
    println(s" → $topCountry with $topCount bookings")

  }

  def question2(): Unit = {
    // which hotel offers the most economical option for customers
    // based on the following criteria?

    // 1. Find the hotel with the lowest booking price
    // `minBy(_.bookingPriceSGD)` returns the HotelData object with the minimum price
    val cheapestBooking = data.minBy(_.bookingPriceSGD)
    println("Cheapest Booking Price Hotel:")
    println(s" → ${cheapestBooking.hotelName}")
    println(s"Booking Price: $$${cheapestBooking.bookingPriceSGD}")
    println()

    // 2. Find the hotel with the highest discount
    // `maxBy(_.discount)` returns the HotelData object with the maximum discount
    val highestDiscount = data.maxBy(_.discount)
    println("Highest Discount Hotel:")
    println(s" → ${highestDiscount.hotelName}")
    println(s"Discount: ${highestDiscount.discount}%")
    println()

    // 3. Find the hotel with the lowest profit margin
    // `minBy(_.profitMargin)` returns the HotelData object with the minimum profit margin
    val lowestProfitMargin = data.minBy(_.profitMargin)
    println("Lowest Profit Margin Hotel:")
    println(s" → ${lowestProfitMargin.hotelName}")
    println(s"Profit Margin: ${lowestProfitMargin.profitMargin}")
    println()

  }

  def question3(): Unit = {
    // which hotel is the most profitable when considering the number of
    // visitor and profit margin?

    // Step 1: Group all bookings by hotel name
    // Creates a Map[String, List[HotelData]] where the key is the hotel name
    val profitability = data.groupBy(_.hotelName).map { case (hotel, bookings) =>

      // Step 2: Calculate total number of visitors per hotel
      // Sum the numberOfPeople field across all bookings
      val totalVisitors = bookings.map(_.numberOfPeople).sum

      // Step 3: Calculate total profit per hotel
      // Multiply profitMargin by numberOfPeople for each booking, then sum
      val totalProfit = bookings.map(b => b.profitMargin * b.numberOfPeople).sum

      // Step 4: Return a tuple of (hotel name, total profit, total visitors)
      (hotel, totalProfit, totalVisitors)
    }

    // Step 5: Find the hotel with the maximum total profit
    // `maxBy(_._2)` compares the totalProfit (second element of tuple)
    val (bestHotel, bestProfit, visitors) = profitability.maxBy(_._2)

    // Step 6: Print the results
    println(s"Most profitable hotel:")
    println(s" → $bestHotel")
    println(f"Total Profit: $$$bestProfit%.2f")
    println(s"Total Visitors: $visitors")
  }

  // output

  println("\n==================== Hotel Booking Analysis ====================\n")

  println(">>> Question 1: Country with the highest number of bookings <<<\n")
  question1()
  println("\n---------------------------------------------------------------\n")

  println(">>> Question 2: Most economical hotels <<<\n")
  question2()
  println("\n---------------------------------------------------------------\n")

  println(">>> Question 3: Most profitable hotel <<<\n")
  question3()
  println("\n==================== End of Analysis =========================\n")

}

/**
 * End of Hotel Booking Analysis Program
 */
