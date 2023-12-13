package org.sunbird.obsrv.dataproducts.util

import java.util
import com.google.gson.Gson
import org.apache.http.client.HttpResponseException
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity

import scala.io.Source
// $COVERAGE-OFF$
trait HTTPService {
  def post(url: String, body: String,  headers: Option[Map[String, String]] = None): (Int, String)
  def delete(url: String, headers: Option[Map[String, String]] = None):(Int, String)

}

case class RestUtil() extends HTTPService with Serializable {

  def post(url: String, body: String, headers: Option[Map[String, String]] = None): (Int, String) = {
    val httpClient = HttpClients.createDefault()
    val request = new HttpPost(url)
    headers.getOrElse(Map()).foreach {
      case (headerName, headerValue) => request.addHeader(headerName, headerValue)
    }
    request.addHeader("Content-Type", "application/json")
    request.setEntity(new StringEntity(body))
    try {
      val httpResponse = httpClient.execute(request.asInstanceOf[HttpRequestBase])
      val entity = httpResponse.getEntity
      val inputStream = entity.getContent
      val status = httpResponse.getStatusLine.getStatusCode
      val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString
      inputStream.close()
      (status, content)
    }

    catch {
      case e: HttpResponseException =>
        val status = e.getStatusCode
        println(s"HTTP request failed with status code: $status")
        (status, e.getMessage)
      case e: Exception =>
        println("Exception while executing post request: " + e.getMessage)
        (500, e.getMessage)
    }
    finally {
      httpClient.close()
    }
  }

  def delete(url: String, headers: Option[Map[String, String]] = None): (Int, String) = {
    val httpClient = HttpClients.createDefault()
    val request = new HttpDelete(url)
    headers.getOrElse(Map()).foreach {
      case (headerName, headerValue) => request.addHeader(headerName, headerValue)
    }
    try {
      val httpResponse = httpClient.execute(request.asInstanceOf[HttpRequestBase])
      val entity = httpResponse.getEntity
      val inputStream = entity.getContent
      val status = httpResponse.getStatusLine.getStatusCode
      val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString
      inputStream.close()
      (status, content)
    }
    catch {
      case e: HttpResponseException =>
        val status = e.getStatusCode
        println(s"HTTP request failed with status code: $status")
        (status, e.getMessage)
      case e: Exception =>
        println("Exception while executing delete request: " + e.getMessage)
        (500, e.getMessage)
    }
    finally {
      httpClient.close()
    }
    // $COVERAGE-ON$
  }

}
