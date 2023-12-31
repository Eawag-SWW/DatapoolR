% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/datapoolR.r
\name{query.database}
\alias{query.database}
\title{Query datapool}
\usage{
query.database(query, ..., showQuery = FALSE,
  allow.modifications = FALSE)
}
\arguments{
\item{query}{a string containing the SQL query}

\item{...}{arguments interpolated in the query,
see \code{link[DBI]{sqlInterpolate}} for details}

\item{showQuery}{if \code{TRUE} print SQL query}

\item{allow.modifications}{FALSE}
}
\value{
a data.frame
}
\description{
Sends a general SQL query to the datapool
}
\details{
Per default only \code{SELECT} queries are allowed (see argument \code{allow.modifications}).
All timestamps are converted in \code{POSIXct} objects with time zone \code{UTC}.
}
\examples{
\dontrun{
query.database("select * from source")
}
}
\author{
Andreas Scheidegger
}
