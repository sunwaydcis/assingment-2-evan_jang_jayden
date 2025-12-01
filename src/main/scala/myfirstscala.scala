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

}
