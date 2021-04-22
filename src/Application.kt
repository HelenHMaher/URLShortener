package com.example

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*
import io.ktor.jackson.*
import java.math.BigInteger
import java.security.MessageDigest


fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

fun String.encodeToID(truncateLength: Int = 6): String {
    // hash String with MD5
    val hashBytes = MessageDigest.getInstance("MD5").digest(this.toByteArray(Charsets.UTF_8))
    val hashString = String.format("%032x", BigInteger(1, hashBytes))
    return hashString.take(truncateLength)
}

data class Request(val url: String) {
    fun toResponse(): Response = Response(url, url.encodeToID())
}

data class Response(val originalURL: String, private val id: String) {
    val shortURL: String = "http://localhost:8080/$id"
}

@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
        }
    }

    // Hash table Response object by ID
    val responseByID = mutableMapOf<String, Response>()

    fun getShortURL(url: String, truncateLength: Int = 6): String {
        val id = url.encodeToID()

        val retrievedResponse = responseByID[id]
        if (retrievedResponse?.originalURL != url) {
            // collision spotted !
            return getShortURL(url, truncateLength +1)
        }
        return id
    }

    routing {

        get("/{id}") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let {responseByID[it]}

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respondRedirect("http://www.google.com")
            }

            log.debug("redirect to: $retrievedResponse")
            call.respondRedirect(retrievedResponse.originalURL)
        }

        post("/api/v1/encode") {
            // Deserialize JSON body to Request object
            val request = call.receive<Request>()

            // find the Response object if it already exists
            val shortURL = getShortURL(request.url)
            val retrievedResponse = responseByID[shortURL]
            if (retrievedResponse != null) {
                log.debug("cache hit $retrievedResponse")
                return@post call.respond(retrievedResponse)
            }

            val response = request.toResponse()
            responseByID[shortURL] = response
            log.debug("cache miss $response")

            // Serialize Response object to JSON body
            call.respond(response)

        }
    }
}


