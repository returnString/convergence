#!/usr/bin/env Rscript

csv <- function(file, ...) {
	write.csv(data.frame(...), paste0("convergence-arrow/data/", file, ".csv"), row.names = F)
}

csv("100_4buckets",
	id = seq(1, 100),
	bucket = c("a", "b", "c", "d")
)
