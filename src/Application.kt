package com.qovery.oss

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*
import io.ktor.jackson.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigInteger
import java.security.MessageDigest
import java.util.*


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

data class Stat(val clicksOverTime: MutableList<Date> = mutableListOf())

data class Response(val originalURL: String, private val id: String, val stat: Stat = Stat()) {
    val shortURL: String = "${System.getenv("QOVERY_APPLICATION_API_HOST")}/$id"
}

object ResponseTable : Table("response") {
    val id = varchar("id", 32)
    val originalURL = varchar("original_url", 2048)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

object ClickOverTimeTable : Table("click_over_time") {
    val id = integer("id").autoIncrement()
    val clickDate = datetime("click_date")
    val response = reference("response_id", onDelete = ReferenceOption.CASCADE, refColumn = ResponseTable.id)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

fun initDatabase() {
    val config = HikariConfig().apply {
//        jbdcUrl = "jbdc:postgresql://127.0.0.1:5432/exposed"
        username = "exposed"
        password = "exposed"
        driverClassName = "org.postgresql.Driver"
    }

    Database.connect(HikariDataSource(config))

    transaction {
        // create tables if they do not exist
        SchemaUtils.createMissingTablesAndColumns(ResponseTable, ClickOverTimeTable)
    }
}

@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    initDatabase()

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }

    // Hash table Response object by ID
    val responseByID = mutableMapOf<String, Response>()

    fun getShortURL(url: String, truncateLength: Int = 6): String {
        val id = url.encodeToID()

        val retrievedResponse = responseByID[id]
        if (retrievedResponse != null && retrievedResponse.originalURL != url) {
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

            retrievedResponse.stat.clicksOverTime.add(Date())

            log.debug("redirect to: $retrievedResponse")
            call.respondRedirect(retrievedResponse.originalURL)
        }

        get("/api/v1/url/{id}/stat") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let {responseByID[it]}

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respond(HttpStatusCode.NoContent)
            }
            call.respond(retrievedResponse.stat)
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

        get("/") {
            call.respondText("Hello, world!")
        }
    }
}


