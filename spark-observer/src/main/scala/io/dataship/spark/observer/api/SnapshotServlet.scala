package io.dataship.spark.observer.api

import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

private[api] final case class SnapshotServletResult(
    statusCode: Int,
    body: String
)

final class SnapshotServlet(service: LiveSnapshotService) extends HttpServlet {
  override protected def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse
  ): Unit = {
    val result = responseFor(Option(request.getParameter("limit")))
    response.setStatus(result.statusCode)
    response.setContentType("application/json")
    response.setCharacterEncoding(StandardCharsets.UTF_8.name())
    response.getWriter.write(result.body)
  }

  private[api] def responseFor(rawLimit: Option[String]): SnapshotServletResult = {
    try {
      val requestedLimit = rawLimit.map(_.toInt)
      SnapshotServletResult(
        statusCode = HttpServletResponse.SC_OK,
        body = JsonRenderer.render(service.snapshot(requestedLimit))
      )
    } catch {
      case _: NumberFormatException | _: InvalidSnapshotLimitException =>
        error(HttpServletResponse.SC_BAD_REQUEST, "INVALID_LIMIT")
      case _: SnapshotStoreNotReadyException =>
        error(HttpServletResponse.SC_CONFLICT, "STORE_NOT_READY")
      case NonFatal(_) =>
        error(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "SNAPSHOT_FAILED")
    }
  }

  private def error(statusCode: Int, errorCode: String): SnapshotServletResult =
    SnapshotServletResult(
      statusCode = statusCode,
      body = JsonRenderer.render(SnapshotErrorResponse(errorCode))
    )
}
