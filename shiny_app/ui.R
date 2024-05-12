require(shiny)

# Basic tab-based UI where each tab corresponds to one of the tasks in Part 1.
# Task N corresponds to Core feature N+1 in the spec.
fluidPage(
  withMathJax(),
  # Application title
  titlePanel("FlightsExplorer"),
  
  # Define tabs
  tabsetPanel(
    
    # Task 1 Tab
    tabPanel("Task 1",
             sidebarLayout(
               sidebarPanel(
                 h1("Task 1"),
                 p("Enter a year or years below to retrieve the total number of flights present in the dataset for said year(s)."),
                 br(),
                 HTML("E.g. Range: <i> 2005-2007 </i> or <i>2005,2006,2007</i>"),
                 textInput("searchQuery", "Enter Flight Year(s):", ""),
                 actionButton("searchButton", "Run Task 1 query")
               ),
               
               mainPanel(
                 tabsetPanel(
                   tabPanel("Plot", plotOutput("numFlightsPerYear")),
                   tabPanel("Table", tableOutput("numFlightsPerYearTable"))
                 )
               )
             )
    ),
    
    # Task 2 Tab
    tabPanel("Task 2",
             sidebarLayout(
               sidebarPanel(
                 h1("Task 2"),
                 p("Enter a year below to retrieve the percentage of flights that departed: "),
                 HTML("<ul><li>On-time</li><li>Early</li><li>Late</li></ul>"),
                 numericInput("task2Year", "Enter Flight Year: ", 1987),
                 actionButton("task2Button","Run Task 2 query")
               ),
               
               mainPanel(
                 plotOutput("numFlightsOnTimeEarlyLate")
               )
             )
    ),
    
    # Task 3 Tab
    tabPanel("Task 3",
             sidebarLayout(
               sidebarPanel(
                 h1("Task 3"),
                p("Enter a year below to retrieve the top cancellation reason for flights that year"),
                numericInput("task3Year", "Enter Flight Year: ", 1987),
                actionButton("task3Button","Run Task 3 query")
             ),
             mainPanel(
               htmlOutput("topCancelReason")
             )
            )
    ),
    
    # Task 4 Tab
    tabPanel("Task 4",
             sidebarLayout(
               sidebarPanel(
                 h1("Task 4"),
                 p("In 1987, 1997, 2007 and 2017, what were the top 3 airports (and where were they) that recorded the most punctual take-offs?"),
                 actionButton("task4Button", "Run Task 4 query")
               ),
               mainPanel(
                  tableOutput("topThreePunctualAirlinesTable")
               )
             )
    ),
    tabPanel("Task 5",
             sidebarLayout(
               sidebarPanel(
                 h1("Task 5"),
                 p("Examining all the years in the 20th century, display the top three worst performing airlines. Define how you interpret the word 'worst'."),
                 actionButton("task5Button", "Run Task 5 query")
               ),
               mainPanel(
                 
                p("To find the three worst performing airlines, I defined a performance penalty metric for a given airline:"),
                p("$$Penalty(airline) = 5 \\times AVG(Cancelled) + 3 \\times AVG(ArrDel15) + 2 \\times AVG(DepDel15)$$"),
                br(),
                p("For a given airline, the penalty is defined in terms of the proportion of cancelled flights, flights that arrived late and flights that departed late."),
                p("We assign the biggest penalty, five, to the proportion of cancelled flights. That is if an airline has many cancelled flights, this metric will reflect it."),
                p("The weights sum up to 10, therefore the penalty is defined between 0 and 10."),
                p("Click 'Run query' to see the airlines with the largest penalty."),
                br(),
                plotOutput("worstAirlines"),
                plotOutput("worstAirlinesPenalties")
               ))
    )
  )
  )
