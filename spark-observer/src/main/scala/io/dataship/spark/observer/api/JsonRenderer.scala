package io.dataship.spark.observer.api

import com.fasterxml.jackson.databind.ObjectMapper

object JsonRenderer {
  private val objectMapper = new ObjectMapper()

  def render(response: HealthResponse): String =
    objectMapper.writeValueAsString(response.allowlistedValues)

  def render(response: CountersResponse): String =
    objectMapper.writeValueAsString(response.allowlistedValues)
}
