/** Hotel Data Analysis Program
 It performs three main analyses:
 1. Finds the country with the highest number of bookings
 2. Identifies the most economical hotel using a scoring method
 3. Identifies the most profitable hotel based on visitors + margins
 */

import com.github.tototoshi.csv.*

// Case class representing one full row of hotel booking data
// Each field corresponds directly to a column in the CSV
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
// Class responsible for reading and parsing the dataset
class HotelDataReader {

  // Reads the CSV file and converts every row into a HotelData object
  def readData(file: String): List[HotelData] = {
    val path: String = getClass.getResource(file).toURI.getPath
    val reader = CSVReader.open(path)

    // allWithHeaders() returns rows as Map[String, String]
    val data = reader.allWithHeaders().map(parseRow)
    reader.close()
    data
  }

  // Converts a single CSV row into a HotelData instance
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

// Normalization utilities
// lowerIsBetter → lower value should get higher score
// higherIsBetter → higher value should get higher score
object Normalizer {
  def lowerIsBetter(value: Double, min: Double, max: Double): Double =
    if (max == min) 100 else (1 - (value - min) / (max - min)) * 100

  def higherIsBetter(value: Double, min: Double, max: Double): Double =
    if (max == min) 100 else ((value - min) / (max - min)) * 100
}

// Scoring logic used in question 2 and question 3
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

  // Read dataset into memory
  // The same function can read any dataset with the same structure
  val data = HotelDataReader().readData("Hotel_Dataset.csv")

  // Question 1 Find country with highest number of bookings
  object question1 {
    def run(data: List[HotelData]): Unit = {
      // Group by destination country
      val grouped = data.groupBy(_.destinationCountry)

      // Count number of bookings per country
      val counts = grouped.mapValues(_.size)

      // Select country with maximum bookings
      val (country, count) = counts.maxBy(_._2)

      println("Country with the highest number of bookings:")
      println(s"→ $country")
      println(s"Bookings: $count")
    }
  }

  // Question 2 Most economical hotel
  // Criteria: low price, high discount, low profit margin
  object question2 {

    // Helper class storing pre-computed averages for each hotel
    case class ProcessedHotel(
                               country: String,
                               name: String,
                               city: String,
                               avgPrice: Double,
                               avgDiscount: Double,
                               avgProfitMargin: Double
                             )

    def run(data: List[HotelData]): Unit = {

      // Group by unique hotel (country + name + city)
      val grouped = data.groupBy(b => (b.destinationCountry, b.hotelName, b.destinationCity))

      // Compute averages for each hotel
      val processed = grouped.map { case ((country, name, city), bookings) =>
        ProcessedHotel(
          country,
          name,
          city,
          bookings.map(_.bookingPriceSGD.toDouble).sum / bookings.size,
          bookings.map(_.discount.toDouble).sum / bookings.size,
          bookings.map(_.profitMargin.toDouble).sum / bookings.size
        )
      }.toList

      // Extract lists for normalization
      val prices = processed.map(_.avgPrice)
      val discounts = processed.map(_.avgDiscount)
      val profits = processed.map(_.avgProfitMargin)

      // Build min + max lists
      val mins = List(prices.min, discounts.min, profits.min)
      val maxs = List(prices.max, discounts.max, profits.max)

      // Flags: false = lower is better, true = higher is better
      val flags = List(false, true, false)

      // Score every hotel based on normalized metrics
      val scored = processed.map { h =>
        val values = List(h.avgPrice, h.avgDiscount, h.avgProfitMargin)
        (h, Scoring.computeScore(values, mins, maxs, flags))
      }

      // Select hotel with highest score
      val (bestHotel, bestScore) = scored.maxBy(_._2)

      // Print output
      println("Most Economical Hotel:")
      println(f" → ${bestHotel.country} - ${bestHotel.name} - ${bestHotel.city}")
      println(f"Average Price: ${bestHotel.avgPrice}%.2f SGD")
      println(f"Average Discount: ${bestHotel.avgDiscount}%.2f%%")
      println(f"Average Profit Margin: ${bestHotel.avgProfitMargin * 100}%.2f%%")
      println(f"Final Score: $bestScore%.2f")
    }
  }

  object question3 {

    case class ProcessedHotel(
                               country: String,
                               name: String,
                               city: String,
                               totalVisitors: Int,
                               avgProfitMargin: Double
                             )

    def run(data: List[HotelData]): Unit = {

      // Group by hotel
      val grouped = data.groupBy(b => (b.destinationCountry, b.hotelName, b.destinationCity))

      // Sum visitors, average profit margin
      val processed = grouped.map { case ((country, name, city), bookings) =>
        ProcessedHotel(
          country,
          name,
          city,
          bookings.map(_.numberOfPeople).sum,
          bookings.map(_.profitMargin.toDouble).sum / bookings.size
        )
      }.toList

      val visitors = processed.map(_.totalVisitors.toDouble)
      val margins = processed.map(_.avgProfitMargin)

      val mins = List(visitors.min, margins.min)
      val maxs = List(visitors.max, margins.max)

      val flags = List(true, true)

      val scored = processed.map { h =>
        val values = List(h.totalVisitors.toDouble, h.avgProfitMargin)
        (h, Scoring.computeScore(values, mins, maxs, flags))
      }

      val (bestHotel, bestScore) = scored.maxBy(_._2)

      println("Most Profitable Hotel:")
      println(f" → ${bestHotel.country} - ${bestHotel.name} - ${bestHotel.city}")
      println(f"Total Visitors: ${bestHotel.totalVisitors}")
      println(f"Average Profit Margin: ${bestHotel.avgProfitMargin * 100}%.2f%%")
      println(f"Final Score: $bestScore%.2f")
    }
  }

  // output

  println("\n==================== Hotel Booking Analysis ====================\n")

  println(">>> Question 1: Country with the highest number of bookings <<<\n")
  question1.run(data)
  println("\n---------------------------------------------------------------\n")

  println(">>> Question 2: Most economical hotel <<<\n")
  question2.run(data)
  println("\n---------------------------------------------------------------\n")

  println(">>> Question 3: Most profitable hotel <<<\n")
  question3.run(data)
  println("\n==================== End of Analysis =========================\n")

}

/**
 * End of Hotel Booking Analysis Program
 */