
mkColletor <- function() {
  d = list()
  count = 0
  width = 0
  pad = c()
  # @param op what to do 1=collect, 2=return what we have
  # @param newdata list or data frame of new data rows
  # @return an efficient incremental collection of data
  function(op,newdata=c())  {
    if(op==2) {
      if(count<=0) {
        return(data.frame())
      }
      return(data.frame(d,stringsAsFactors=FALSE)[seq_len(count),])
    }
    ncol <- length(newdata)
    if(ncol<1) {
      return(c())
    }
    nrow <- length(newdata[[1]])
    if(nrow<1) {
      return(c())
    }
    if(ncol>1) {
      lens <- vapply(newdata,length,numeric(1))
      if(max(lens)!=min(lens)) {
        stop("ragged new data to collect")
      }
    }
    if(count<=0) {
      d <<- data.frame(newdata,stringsAsFactors=FALSE)
      width <<- ncol
      count <<- nrow
      pad <<- d[1,]
      pad[1,] <<- NA
      d <<- rbind(d,pad[rep.int(1,100),100+2*count])
      d <<- as.list(d) # lose data.frame class for speed
      seqj <<- seq_len(ncol)
    } else {
      if(width!=ncol) {
        stop("collect wrong number of columns to collect")
      }
      nalloc <- length(d[[1]])
      if(count+nrow>nalloc) {
        d <<- data.frame(d,stringsAsFactors=FALSE)
        d <<- rbind(d,pad[rep.int(1,2*nalloc+nrow),])
        d <<- as.list(d) # lose data.frame class for speed
      }
      for(i1 in seq_len(nrow)) {
        i2 <- count + i1
        for(j in seqj) {
          d[[j]][[i2]] <<- newdata[[j]][[i1]]
        }
      }
      count <<- count + nrow
    }
    c()
  }
}



#' @param collector from mkColletor()
#' @newdata list or data frame of new data rows
#' @return an efficient incremental collector of data
collectRows <- function(collector,newdata) {
   collector(1,newdata)
}

#' @param collector from mkColletor()
#' @return data frame
unwrapRows <- function(collector) {
   collector(2)
}


