/** Hotel Data Analysis Program
 *
 * This program analyzes hotel booking dataset provided by a company that assists
 * international travelers in securing accommodations across various destination countries
 */

import com.github.tototoshi.csv.*

// class for reading and parsing data

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

class HotelDataReader {

  // class for data encapsulation



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
object Normalizer {
  def lowerIsBetter(value: Double, min: Double, max: Double): Double =
    if (max == min) 100 else (1 - (value - min) / (max - min)) * 100

  def higherIsBetter(value: Double, min: Double, max: Double): Double =
    if (max == min) 100 else ((value - min) / (max - min)) * 100
}

object Scoring {
  def computeScore(values: List[Double], mins: List[Double], maxs: List[Double], higherIsBetter: List[Boolean]): Double = {
    val scores = values.indices.map { i =>
      if (higherIsBetter(i))
        Normalizer.higherIsBetter(values(i), mins(i), maxs(i))
      else
        Normalizer.lowerIsBetter(values(i), mins(i), maxs(i))
    }
    scores.sum / scores.size
  }
}

@main
def main(): Unit = {

  // read data

  // enter file name to allow for usage of the same function for
  // dataset with different names
  val data = HotelDataReader().readData("Hotel_Dataset.csv")

  // questions

  def question1(): Unit = {
    // which country has the highest number of bookings in the dataset?

    // Step 1: Group all bookings by the destination country
    // This creates a Map[String, List[HotelData]] where the key is the country
    val grouped = data.groupBy(_.destinationCountry)

    // Step 2: Count the number of bookings per country
    // `mapValues(_.size)` transforms each list of bookings into its length
    val counts = grouped.mapValues(_.size)

    // Step 3: Find the country with the maximum number of bookings
    // `maxBy(_._2)` compares the counts (second element of the tuple) and returns the highest
    val (topCountry, topCount) = counts.maxBy(_._2)

    // Step 4: Print the result
    println(s"Country with the highest number of bookings:")
    println(s" → $topCountry ")
    println(s"Bookings: $topCount ")

  }

  def question2(): Unit = {
    val grouped = data.groupBy(b => (b.destinationCountry, b.hotelName, b.destinationCity))

    case class ProcessedHotel(
                               country: String,
                               name: String,
                               city: String,
                               avgPrice: Double,
                               avgDiscount: Double,
                               avgProfitMargin: Double,
                               numTransactions: Int
                             )

    val processed = grouped.map { case ((country, name, city), bookings) =>
      ProcessedHotel(
        country,
        name,
        city,
        bookings.map(_.bookingPriceSGD.toDouble).sum / bookings.size,
        bookings.map(_.discount.toDouble).sum / bookings.size,
        bookings.map(_.profitMargin.toDouble).sum / bookings.size,
        bookings.size
      )
    }.toList

    // min–max for 3 metrics
    val prices = processed.map(_.avgPrice)
    val discounts = processed.map(_.avgDiscount)
    val profits = processed.map(_.avgProfitMargin)

    val mins = List(prices.min, discounts.min, profits.min)
    val maxs = List(prices.max, discounts.max, profits.max)

    // higherIsBetter flags
    val flags = List(false, true, false) // price ↓ , discount ↑ , profit ↓

    val scored = processed.map { h =>
      val values = List(h.avgPrice, h.avgDiscount, h.avgProfitMargin)
      val score = Scoring.computeScore(values, mins, maxs, flags)
      (h, score)
    }

    val (bestHotel, bestScore) = scored.maxBy(_._2)

    println("Most Economical Hotel:")
    println(f" → ${bestHotel.country} - ${bestHotel.name} - ${bestHotel.city}")
    println(f"Final Score: $bestScore%.2f")
  }

  def question3(): Unit = {
    val grouped = data.groupBy(b => (b.destinationCountry, b.hotelName, b.destinationCity))

    case class ProcessedHotel(
                               country: String,
                               name: String,
                               city: String,
                               totalVisitors: Int,
                               avgProfitMargin: Double
                             )

    val processed = grouped.map { case ((country, name, city), bookings) =>
      ProcessedHotel(
        country,
        name,
        city,
        bookings.map(_.numberOfPeople).sum,
        bookings.map(_.profitMargin.toDouble).sum / bookings.size
      )
    }.toList

    // min–max for visitors & margins
    val visitors = processed.map(_.totalVisitors.toDouble)
    val margins = processed.map(_.avgProfitMargin)

    val mins = List(visitors.min, margins.min)
    val maxs = List(visitors.max, margins.max)

    val flags = List(true, true) // visitors ↑, margin ↑

    val scored = processed.map { h =>
      val values = List(h.totalVisitors.toDouble, h.avgProfitMargin)
      val score = Scoring.computeScore(values, mins, maxs, flags)
      (h, score)
    }

    val (bestHotel, bestScore) = scored.maxBy(_._2)

    println("Most Profitable Hotel:")
    println(f" → ${bestHotel.country} - ${bestHotel.name} - ${bestHotel.city}")
    println(f"Total Visitors: ${bestHotel.totalVisitors}")
    println(f"Avg Margin: ${bestHotel.avgProfitMargin}%.2f")
    println(f"Final Score: $bestScore%.2f")
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