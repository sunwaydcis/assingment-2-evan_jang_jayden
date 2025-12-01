/** Hotel Data Analysis Program
 *
 * This program analyzes hotel booking dataset provided by a company that assists
 * international travelers in securing accommodations across various destination countries
 */

import com.github.tototoshi.csv.*

// class for reading nad parsing data

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

  val data = HotelDataReader().readData("Hotel_Dataset.csv")

  // questions

  def question1(): Unit = {
    // which country has the highest number of bookings in the dataset?

    val grouped = data.groupBy(_.originCountry)
    val counts = grouped.mapValues(_.size)

    val (topCountry, topCount) = counts.maxBy(_._2)

    println(s"Country with the highest number of bookings:")
    println(s" → $topCountry with $topCount bookings")

  }

  def question2(): Unit = {
    // which hotel offers the most economical option for customers
    // based on the following criteria?

    // 1. Most economical based on booking price (lowest)
    val cheapestBooking = data.minBy(_.bookingPriceSGD)
    println("Cheapest Booking Price Hotel:")
    println(s" → ${cheapestBooking.hotelName}")
    println(s"Booking Price: $$${cheapestBooking.bookingPriceSGD}")
    println()

    // 2. Most economical based on discount (highest)
    val highestDiscount = data.maxBy(_.discount)
    println("Highest Discount Hotel:")
    println(s" → ${highestDiscount.hotelName}")
    println(s"Discount: ${highestDiscount.discount}%")
    println()

    // 3. Most economical based on profit margin (lowest)
    val lowestProfitMargin = data.minBy(_.profitMargin)
    println("Lowest Profit Margin Hotel:")
    println(s" → ${lowestProfitMargin.hotelName}")
    println(s"Profit Margin: ${lowestProfitMargin.profitMargin}")
    println()

  }

  def question3(): Unit = {
    // which hotel is the most profitable when considering the number of
    // visitor and profit margin?

    val profitability = data.groupBy(_.hotelName).map { case (hotel, bookings) =>

      val totalVisitors = bookings.map(_.numberOfPeople).sum
      val totalProfit =
        bookings.map(b => b.profitMargin * b.numberOfPeople).sum

      (hotel, totalProfit, totalVisitors)
    }

    val (bestHotel, bestProfit, visitors) = profitability.maxBy(_._2)

    println(s"Most profitable hotel:")
    println(s" → $bestHotel")
    println(f"Total Profit: $$$bestProfit%.2f")
    println(s"Total Visitors: $visitors")
  }

  // output

  println()
  println("——————————————————————————————————————————————————")
  println("Question 1")
  println("——————————————————————————————————————————————————")
  println()
  question1()
  println()
  println("——————————————————————————————————————————————————")
  println("Question 2")
  println("——————————————————————————————————————————————————")
  println()
  question2()
  println()
  println("——————————————————————————————————————————————————")
  println("Question 3")
  println("——————————————————————————————————————————————————")
  println()
  question3()
  println()

}

/** Footer Documentation
 * End of Hotel Booking Analysis Program
 */
