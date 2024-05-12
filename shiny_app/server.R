#
# This is the server logic of a Shiny web application. You can run the
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

# Import all libraries needed
require(shiny)
require(bslib)
require(promises)
require(future)
require(httr)
require(jsonlite)
require(tidyverse)


#Constants
API_PORT <- 5000
API_URL <- sprintf("http://localhost:%d/part1/", API_PORT)


getJsonData <- function(endpoint, param=NULL) {
  #' Performs an asynchronous GET request
  #' @endpoint the API endpoint to send the request to
  #' @param a parameter to include after the endpoint
  #' 
  #' Returns:
  #'  A promise containing the parsed JSON response.
  #'  
  #' Throws:
  #'  An unhandled promise error if the server did not return 200 OK
  #'  
  #' Example: getJsonData(endpoint="hello", param="world")
  #' This will connect to the following URL: <API_URL>/hello/world
  future_promise({
    
    #construct URI to hit.
    uri <- paste0(API_URL, endpoint)
    if (!is.null(param)) {
      uri <-paste0(uri, "/", param)
    }
    
    print(uri)
    response <-httr::GET(uri) # perform GET request
    
    if (http_status(response)$category == "Success") { #If server returned 200 OK
      # Parse JSON response
      json_data <- content(response, as = "text")
      parsed_data <- jsonlite::fromJSON(json_data)
      
      # Return parsed JSON as a list or a dataframe.
      print(parsed_data)
      return(parsed_data)
    }
    else { # Raise exception with response.
      stop(response)
    }
  })
}

postJsonData <- function(endpoint, payloadList) {
  #' Performs an asynchronous POST request
  #' @endpoint the API endpoint to send the request to
  #' @payloadList a list to include as the JSON payload of the POST request
  #' 
  #' Returns:
  #'  A promise containing the parsed JSON response.
  #'  
  #' Throws:
  #'  An unhandled promise error if the server did not return 200 OK
  #'  
  #' Example: getJsonData(endpoint="stats", payloadList=list(year = 2019, agg = "median"))
  #'  This will send to the following URL: <API_URL>/stats, the following JSON:
  #'  {"year" : 2019, "agg" : "median"}
  future_promise({
    uri <- paste0(API_URL, endpoint)
#    payloadList$isRange <- unbox(payloadList$isRange)
    json_body <- jsonlite::toJSON(payloadList, auto_unbox = F)
    
    print(json_body)
    response <- httr::POST(uri, content_type_json(), body = json_body)
    
    if (http_status(response)$category == "Success") {
      # Parse JSON response
      json_data <- content(response, as = "text")
      parsed_data <- jsonlite::fromJSON(json_data)
      
      # Return parsed data
      return(parsed_data)
    } 
    else {
      stop(response)
    }
    
  })
}

extractDatesFromFlightQuery <- function(query) {
  #' Extracts flight dates from a search query
  #' Valid flight years are in [1987, 2020]; any other years don't match
  #' @query the search query
  #' 
  #' Returns:
  #'  A list containing: 
  #'   years : list() = list of valid flights years
  #'   isRange :logi = a boolean denoting whether the years list is a range or distinct values.
  #'
  # If query is <year1>-<year2>, extract <year1> and <year2> iff 1987<=year1<2020 and 1987<=year2<=2020
  rangePattern <- "^(198[789]|199[0-9]|20[01][0-9]|2020)-(198[789]|199[0-9]|20[01][0-9]|2020)$"
  
  # If query is comma-separated sequence of valid years, extract each year
  # Note this regex skips over invalid years or strings. E.g. if query="2020, blahblah, 1986, 1989", we extract only: 2020 and 1989.
  csvPattern <- "(?:^|[ ,])(198[789]|199[0-9]|20[01][0-9]|2020)"
  
  matches <- stringr::str_match(query, rangePattern)
  
  isRange <- unbox(T) # Make sure isRange is True (a primitive) and not [True], a vector of length 1.
  if (is.na(matches[[1]])) { # no match for range
    isRange <- unbox(F)
    matches <- str_match_all(query, csvPattern)[[1]]
    if (length(matches) == 0) { # no math for comma seperated years
      return(NULL)
    }
  } 
  years <- as.integer(matches[, -1]) # convert string vector to integer vector
  
  return(list(
    years = years,
    isRange = isRange
  ))
}


# Define server logic required to draw a histogram
function(input, output, session) {
  
  
  ### Task 1 Server Logic ###
  observeEvent(input$searchButton, {
    
    # Regex-match the search query
    parsedSearchInput<- extractDatesFromFlightQuery(isolate(input$searchQuery))
    
    # If any of the regex match was succesful, POST query to server.
    if(!is.null(parsedSearchInput)) {
      
      # get results from post body
      data <- reactive({postJsonData("num_flights_took_place", parsedSearchInput)})
      
      # Create copy of data with the Year as a factor
      factoredData <- reactive({data() %...>% (function(df) {
        df$Year <- factor(df$Year)
        return(df)
      })})
      
      if (parsedSearchInput$isRange) { #If the user specified a range, show a line plot
        output$numFlightsPerYear <- renderPlot({
          data() %...>% {
            ggplot(., aes(y=count, x=Year)) +
              geom_point() +
              geom_line() +
              xlab("Flight Year") + ylab("Number of Flights") +
              ggtitle("Line plot of number of flights over time")
          }
        })
      } else { # Otherwise show a horizontal bar chart of counts of flights over years
        output$numFlightsPerYear <- renderPlot({
          factoredData() %...>% {
            ggplot(.) + 
              geom_bar(aes(y=Year, x=count), stat="identity", width=0.5, fill="steelblue") +
              xlab("Number of Flights") + ylab("Year") +
              ggtitle("Bar chart of counts of flights in specified years")
          }
        })
      }
      
      output$numFlightsPerYearTable <- renderTable(factoredData())
    }
  })
  
  ### Task 2 server logic ###
  observeEvent(input$task2Button, {
    year <- as.integer(isolate(input$task2Year))
    
    if ((1987 <= year) && (year <= 2020)) { # client-side check for year bounds.
      
      numFlightsOnTimeEarlyLate <- reactive({getJsonData("num_flights_ontime_early_late", year)})
      departureTypeData <- reactive({
        numFlightsOnTimeEarlyLate() %...>% (function(l) { #jsonlite returns a list but we want a dataframe
          return(data.frame(# Manually convert list from wide format to long format
            departureType = c("Early", "Late", "On-time"),
            proportion=c(l$num_early, l$num_late, l$num_ontime),
            year=rep(l$year, 3) 
          ))
        })
      })
      
      output$numFlightsOnTimeEarlyLate <- renderPlot({
        departureTypeData() %...>% (function(df) { # create pie chart; round proportions to 3 decimal places and multiply by 100.
          ggplot(df, aes(x="", y=proportion, fill=departureType)) +
            geom_bar(stat="identity", width=1) +
            geom_text(aes(label=paste0(round(proportion, 3)*100, "%")), position=position_stack(vjust=0.5)) +
            labs(fill = "Departure Category") +
            coord_polar("y", start=0) +
            theme_void()
        })
      })
      
      
    }})
  
  ### Task 3 Server Logic ###
  observeEvent(input$task3Button, {
    year <- as.integer(isolate(input$task3Year))
    if ((1987 <= year) && (year <= 2020)) { ## client-side check for year
      cancelData <- reactive({getJsonData("get_top_cancel_reason", year)})
      output$topCancelReason <- renderUI({
        cancelData() %...>% (function(l) {
          h3(sprintf("The top cancellation reason for %d is %s.", l$year, l$cancellation_reason))
        })
      })
    }
  })
  
  ### Task 4 Server Logic ###
  observeEvent(input$task4Button, {
    
    punctualData <- reactive({getJsonData("get_top_three_punctual_airlines")}) # punctualData should be auto-coerced to a dataframe automatically.
  
    output$topThreePunctualAirlinesTable <- renderTable(punctualData())
  })
  
  ### Task 5 Server Logic ###
  observeEvent(input$task5Button, {
    
    worstAirlineData <- reactive({getJsonData("get_top_three_worst_airlines")}) # punctualData should be coerced to a dataframe automatically.
    
    longWorstData <- reactive({ # convert airline data from long format to wide format; rename columns to make them less technical.
      worstAirlineData() %...>% {
        rename(.,all_of(c(arrived_late = "avg(ArrDel15)", cancelled = "avg(Cancelled)", departed_late = "avg(DepDel15)"))) %>%
          pivot_longer(cols=c("arrived_late", "cancelled", "departed_late"), names_to="penalty_type")
        }
      })
    # Plot grouped bar chart of average num of cancelled, late take-off and late-arrival flights.
    output$worstAirlines <- renderPlot({
      longWorstData() %...>% {
        ggplot(., aes(x=airline_name, y=value, fill=penalty_type)) +
          geom_bar(stat = "identity", position = "dodge") +
          xlab("Airline name") + ylab("Proportion") +
          ggtitle("Comparison of top three worst airlines") +
          theme_minimal()
      }
    })
    # Plot bar chart of penalties assigned to top three worst airlines.
    output$worstAirlinesPenalties <- renderPlot({
      longWorstData() %...>% {
        ggplot(., aes(x=airline_name, y=performance_penalty)) +
          geom_bar(stat = "identity", position = "dodge", fill="skyblue") +
          xlab("Airline name") + ylab("Performance Penalty") +
          ggtitle("Performance penalties of top three worst airlines") +
          theme_minimal()
      }
    })
  })
  
}
    



