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
      row("Discount").dropRight(1).toDouble, // Parse single CSV row into HotelData; remove '%' from discount before converting to Double
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
  // ========================================
  //       REUSABLE NORMALIZATION UTILS
  // ========================================

  // Normalizes a sequence of numbers using min-max scaling
  def normalize(values: Seq[Double]): Seq[Double] = {
    val min = values.min
    val max = values.max
    if (max - min == 0) values.map(_ => 1.0) // prevent divide by zero
    else values.map(v => (v - min) / (max - min))
  }

  // For criteria where LOWER is better (price, profit margin)
  // Uses TRUE min-max normalization + inversion
  def normalizeLowerBetter(values: Seq[Double]): Seq[Double] = {
    val min = values.min
    val max = values.max
    if (max - min == 0) values.map(_ => 1.0)
    else values.map(v => 1 - ((v - min) / (max - min)))
  }

  // Groups by hotel and computes mean of a function
  def meanByHotel[T](data: List[HotelData], metric: HotelData => Double): Map[String, Double] =
    data.groupBy(_.hotelName).map { case (hotel, bookings) =>
      val avg = bookings.map(metric).sum / bookings.size
      (hotel, avg)
    }

  // Given a Map(hotel -> rawValue), returns Map(hotel -> normalizedValue)
  def normalizeMapLowerBetter(input: Map[String, Double]): Map[String, Double] = {
    val values = input.values.toSeq
    val norm = normalizeLowerBetter(values)
    input.keys.zip(norm).toMap
  }

  def normalizeMapHigherBetter(input: Map[String, Double]): Map[String, Double] = {
    val values = input.values.toSeq
    val norm = normalize(values)
    input.keys.zip(norm).toMap
  }

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
    println(s" → $topCountry with $topCount bookings")

  }

  def question2(): Unit = {
    println("Most Economical Hotel (Price/Room + Discount + Profit Margin):\n")

    // ---------- 1. Compute raw values ----------
    val pricePerRoomPerDayMap = meanByHotel(data, b =>
      (b.bookingPriceSGD.toDouble) / (b.rooms * b.numberOfDays)
    )

    val discountMap = meanByHotel(data, _.discount.toDouble)
    val profitMarginMap = meanByHotel(data, _.profitMargin.toDouble)

    // ---------- 2. Normalize ----------
    val normPrice = normalizeMapLowerBetter(pricePerRoomPerDayMap)
    val normDiscount = normalizeMapHigherBetter(discountMap)
    val normProfit = normalizeMapLowerBetter(profitMarginMap)

    // ---------- 3. Combine ----------
    val combinedScores = pricePerRoomPerDayMap.keys.map { hotel =>
      val finalScore = (normPrice(hotel) + normDiscount(hotel) + normProfit(hotel)) / 3
      (hotel, finalScore)
    }.toMap

    // ---------- 4. Select best ----------
    val (bestHotel, bestScore) = combinedScores.maxBy(_._2)

    println(s"Best all-around economical hotel:")
    println(s" → $bestHotel")
    println(f"Final Score: $bestScore%.4f")
  }

  def question3(): Unit = {
    println("Most Profitable Hotel (Visitors × Profit Margin):\n")

    // 1. Compute raw total profit per hotel
    val totalProfitMap = data.groupBy(_.hotelName).map { case (hotel, bookings) =>
      val totalProfit = bookings.map(b => b.profitMargin.toDouble * b.numberOfPeople).sum
      (hotel, totalProfit)
    }

    // 2. Normalize (higher better)
    val normalizedProfitMap = normalizeMapHigherBetter(totalProfitMap)

    // 3. Select best
    val (bestHotel, bestScore) = normalizedProfitMap.maxBy(_._2)

    println("Most profitable hotel:")
    println(s" → $bestHotel")
    println(f"Profit Score: $bestScore%.4f")
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
