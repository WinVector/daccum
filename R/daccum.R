
newCollector <- function() {
  collection <- new.env(hash=TRUE,parent=emptyenv())
  assign('d',list(),envir=collection)
  assign('count',0,envir=collection)
  collection
}

#' @param collection from newCollection()
#' @newdata list or data frame of new data rows
#' @return an efficient incremental collection of data
collect <- function(collection,newdata) {
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
  count <- get('count',envir=collection,inherits=FALSE)
  d <- get('d',envir=collection,inherits=FALSE)
  # try to make sure ref count is small to get in-place object mutation
  rm(list=c('d','count'),envir=collection)
  if(count<=0) {
    d <- data.frame(newdata,stringsAsFactors=FALSE)
    width <- ncol
    assign('width',width,envir=collection)
    count <- nrow
    pad <- d[1,]
    pad[1,] <- NA
    assign('pad',pad,envir=collection)
    d <- rbind(d,pad[rep.int(1,100),100+2*count])
    d <- as.list(d) # lose data.frame class for speed
  } else {
    width <- get('width',envir=collection,inherits=FALSE)
    if(width!=ncol) {
      stop("collect wrong number of columns to collect")
    }
    nalloc <- length(d[[1]])
    if(count+nrow>nalloc) {
      d <- data.frame(d,stringsAsFactors=FALSE)
      pad <- get('pad',envir=collection,inherits=FALSE)
      d <- rbind(d,pad[rep.int(1,2*nalloc+nrow),])
      d <- as.list(d) # lose data.frame class for speed
    }
    seqj <- seq_len(ncol)
    for(i1 in seq_len(nrow)) {
      i2 <- count + i1
      for(j in seqj) {
        d[[j]][[i2]] <- newdata[[j]][[i1]]
      }
    }
    count <- count + nrow
  }
  assign('d',d,envir=collection)
  assign('count',count,envir=collection)
  print(pryr::address(d))
  c()
}

#' @param collection from newCollection()
#' @return data frame
unwrap <- function(collection) {
  if(is.null(collection)) {
    return(data.frame())
  }
  d <- get('d',envir=collection,inherits=FALSE)
  count <- get('count',envir=collection,inherits=FALSE)
  data.frame(d,stringsAsFactors=FALSE)[seq_len(count),]
}


mkRow <- function(nCol) {
  x <- as.list(rnorm(nCol))
  # make row mixed types by changing first column to string
  x[[1]] <- ifelse(x[[1]]>0,'pos','neg')
  names(x) <- paste('x',seq_len(nCol),sep='.')
  x
}

mkFrameCollect <- function(nRow,nCol) {
  col <- newCollector()
  for(i in seq_len(nRow)) {
    ri <- mkRow(nCol)
    collect(col,ri)
  }
  unwrap(col)
}



