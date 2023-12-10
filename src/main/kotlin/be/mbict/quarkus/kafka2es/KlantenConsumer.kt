package be.mbict.quarkus.kafka2es

import io.vertx.core.json.JsonObject
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.reactive.messaging.Incoming
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient

@ApplicationScoped
class KlantenConsumer {

    @Inject
    lateinit var esClient: RestClient

    @Incoming("klanten")
    fun consume(klant: Klant) {
        val request = Request("PUT", "/klanten/_doc/${klant.id}")
        request.setJsonEntity(JsonObject.mapFrom(klant).toString())
        esClient.performRequest(request)
    }
}

data class Klant(
    val id: Int,
    val naam: String,
    val voornaam: String
)