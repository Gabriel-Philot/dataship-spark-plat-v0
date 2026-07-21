package io.dataship.spark.observer.api

import java.nio.charset.StandardCharsets

import io.dataship.spark.observer.ObserverRuntime
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

final class CountersServlet(runtime: ObserverRuntime) extends HttpServlet {
  override protected def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse
  ): Unit = {
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    response.setCharacterEncoding(StandardCharsets.UTF_8.name())
    response.getWriter.write(JsonRenderer.render(runtime.countersResponse))
  }
}
