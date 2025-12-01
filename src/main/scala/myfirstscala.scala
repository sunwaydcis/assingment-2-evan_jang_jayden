import com.github.tototoshi.csv.*

class HotelDataReader {

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
    profitMargin: BigDecimal
  )

  def readData(file: String): List[HotelData] = {
    val path: String = getClass.getResource(file).toURI.getPath
    val reader = CSVReader.open(path)
    val data = reader.allWithHeaders().map(parseRow)
    reader.close()
    data
  }

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
