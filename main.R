check_and_install_packages <- function(packages) {
  
  options(repos = c(CRAN = "https://mirrors.bfsu.edu.cn/CRAN/"))
  
  for (package_name in packages) {
    
    if (!require(package_name, character.only = TRUE)) {
      message(paste("Package", package_name, "not found. Installing from BFSU CRAN mirror..."))
      install.packages(package_name, dependencies = TRUE)
      
      if (!require(package_name, character.only = TRUE)) {
        stop(paste("Failed to install and load package", package_name))
      }
    } else {
      message(paste("Package", package_name, "is already installed and loaded."))
    }
  }
  
  message("All packages are installed and loaded.")
}

requied_packages <- c(
  "dplyr", 
  "ggplot2", 
  "tidyr", 
  "broom",
  "vroom",
  "here",
  "lubridate",
  "zoo",
  "tidyverse",
  "sandwich",
  "DBI",
  "RPostgres",
  "glue",
  "purrr",
  "furrr",
  "here",
  "moments"
)
check_and_install_packages(requied_packages)

setwd(here::here())

cr <- function(filename) {
  data <- vroom(here("data", filename))
  return(data)
}

cw <- function(dataframe,filename){
  data <- vroom_write(dataframe, here("data", filename))
  return(data)
} 

ts_diff <- function(input_file, output_file, date_column, target_column) {
  if (!file.exists(here("data", input_file))) {
    stop("File does not exist！")
  }
  
  data <- cr(input_file)

  if (!(date_column %in% names(data))) {
    stop("Column does not exist！")
  }
  data[[date_column]] <- ymd(data[[date_column]])
  
  if (!(target_column %in% names(data))) {
    stop("Column does not exist！")
  }
  
  diff_col_name <- paste0("d", target_column) 
  data[[diff_col_name]] <- c(NA, diff(data[[target_column]]))
  diff <- data[, c(date_column, diff_col_name)]
  original_data <- data[, c(date_column, target_column)]
  cw(diff, output_file)
  
  return(list(
    original = original_data, 
    diff = diff             
  ))
}

vix <- ts_diff(
  input_file = here("vix.csv"),
  output_file = here("dVIX.csv"),
  date_column = "date",
  target_column = "vix"
)

vvix <- ts_diff(
  input_file = "vvix_2.csv",
  output_file = "dVVIX.csv",
  date_column = "date",
  target_column = "vvix"
)

dvix$date <- ymd(dvix$date)

wrds <- dbConnect(Postgres(),
                  host = "wrds-pgdata.wharton.upenn.edu",
                  port = 9737,
                  dbname = "wrds",
                  sslmode = "require",
                  user = " ",
                  password = " ")

query <- function(variable_list, database, data, conditions) {
  query_str <- glue("
    SELECT {variable_list}
    FROM {database}
    WHERE date BETWEEN '{start_date}' AND '{end_date}' 
    {conditions}
  ")
  
  res <- dbSendQuery(wrds, query_str)
  fetched_data <- dbFetch(res)
  dbClearResult(res)
  
  if ("date" %in% names(fetched_data)) {
    fetched_data$date <- as.Date(fetched_data$date, format = "%Y-%m-%d")
  }

output_path <- glue(here("data","{data}.csv"))
  write.csv(fetched_data, file = output_path, row.names = FALSE)
  return(fetched_data)
}
hxz <- query(
  variable_list = "q_factors_daily",
  database = "macrofin",
  data = "hxz",
  conditions = ""
)

variable_list <- "
    DATE,
    PERMNO, 
    RET,
    PRC,
    SHROUT,
    VOL
    "
conditions <- "
    AND
    PERMNO NOT IN (
    SELECT DISTINCT PERMNO
    FROM crsp.msf
    WHERE PRC < 1
      OR ABS(PRC) * SHROUT * 1000 < 225000000
)
"
ret <- query(
  variable_list = variable_list,
  database = "crsp.msf",
  data = "ret_monthly",
  conditions = ""
)
ret <- query(
  variable_list = variable_list,
  database = "crsp.dsf",
  data = "ret_daily",
  conditions = ""
)

hxz <- cr("hxz.csv")
dvix <- cr("dVIX.csv")
dvix$date <- ymd(dvix$date)
dvvix <- cr("dVVIX.csv")
ff5 <- cr("ff5.csv")

stocks <- cr("stocks.csv")
head(stocks)
stocks <- stocks %>%
  filter(!(SICCD > 6720 & SICCD < 6730) | SICCD != 6798) %>%
  filter(SHRCD %in% c(10, 11))|>
  mutate(
    MC = PRC * SHROUT
  )

stocks_m <- cr("stocks_monthly.csv")
stocks_m <- stocks_m %>%
  filter(!(SICCD > 6720 & SICCD < 6730) & SICCD != 6798) %>%
  filter(SHRCD %in% c(10, 11))|>
  mutate(
    MC = PRC * SHROUT
  )

columns_to_keep <- c("PERMNO","date","PRC","SHROUT","MC")
stocks <- stocks[,c("PERMNO","date","RET","MC")]
stocks_m <- stocks_m[, columns_to_keep]


stocks_m <- stocks_m %>% 
  mutate(month = format(date %m+% months(1), "%Y-%m")) %>%
  select(-date)
stocks <- stocks %>%
  mutate(month = format(date, "%Y-%m"))

filtered_monthly <- stocks_m %>%
  group_by(month, PERMNO) %>%
  filter(!any(PRC < 1 | PRC * SHROUT * 1000 <= 2.25e8)) %>%
  ungroup() %>% 
  select(PERMNO,month) %>%
  arrange(month)

stocks_f <- merge(stocks, filtered_monthly, by = c("PERMNO", "month")) %>%
  arrange(PERMNO,date)
head(stocks_f)

data_list <- list(stocks_f, dvvix, dvix, ff5, hxz)
merged_ret <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE), data_list)
merged_ret <- na.omit(merged_ret)
merged_ret$RET <- as.numeric(as.character(merged_ret$RET))
merged_ret$er <- merged_ret$RET - merged_ret$rf
cw(merged_ret,"merged_ret.csv")
merged_ret <- cr("merged_ret.csv")
period <- c(unique(merged_ret$month))[-(1:12)]
merged_ret <- merged_ret %>%
  filter( month(date) %in% period) %>%
  group_by(PERMNO) %>%  
  mutate(
    VRP = map_dbl(date, ~ {
      start_day <- .x %m-% days(30)
      
      filtered_data <- stocks %>%
        filter(PERMNO == cur_group_id() & date >= start_day & date < .x)
      EV <- mean(sqrt(filtered_data$RET), na.rm = TRUE)
      IV <- vix %>%
        filter(date == .x) %>%
        pull(vix)
      
      if (!is.na(IV)) {
        return(EV - IV)
      } else {
        return(NA)
      }
    }) 
  ) %>%
  mutate(
    dVRP = c(NA, diff(VRP))
  ) %>%
  ungroup()

portfolio_sort <- function(merged_ret, time){
  results <- data.frame()
  for ( month in time ){
    
    start_date <- as.Date(paste0(month,"-01"))
    end_date <- start_date %m+% years(1)
    
    window_data <- merged_ret %>%
      filter(
        date >= start_date 
        & date < end_date
      )
    
    assets <- window_data %>%
      group_by(PERMNO) %>% 
      filter(n() >= 100) 
    asset_results <- assets %>% 
      do({
        model <- lm(er ~ mktrf + dvix + dvvix + dvrp, data = .) 
        tidy(model)                                    
      })
    
    reg_result <- asset_results %>%
      select(PERMNO, term, estimate) %>%
      pivot_wider(
        names_from = term, 
        values_from = estimate
      ) %>% 
      select(PERMNO, dvvix, dvix) %>% 
      na.omit() %>% 
      mutate(
        beta_V = as.numeric(dvvix * 100),
        beta_Q = dvix,
        beta_R = dvrp,
        month = month
      )
    dvvix_tiles <- quantile(reg_result$beta_V, 
                            probs = seq(0 ,1 , by = 0.2), 
                            na.rm = TRUE
    )

    reg_result$dvvix_ntile <- as.integer(
      cut(reg_result$beta_V, 
          breaks = dvvix_tiles, 
          include.lowest = TRUE, 
          labels = FALSE
      ))
    reg_result$dvvix <- NULL    
    dvix_tiles <- quantile(reg_result$beta_Q, 
                           probs = seq(0 ,1 , by = 0.2), 
                           na.rm = TRUE
    )
    
    reg_result$dvix_ntile <- as.integer(
      cut(reg_result$beta_Q, 
          breaks = dvix_tiles, 
          include.lowest = TRUE, 
          labels = FALSE
      ))
    reg_result$dvix <- NULL
    dvvix_tiles <- quantile(reg_result$beta_V, 
                            probs = seq(0 ,1 , by = 0.2), 
                            na.rm = TRUE
    )
    
    reg_result$dvrp_ntile <- as.integer(
      cut(reg_result$beta_R, 
          breaks = dvrp_tiles, 
          include.lowest = TRUE, 
          labels = FALSE
      ))
    reg_result$dvrp <- NULL   
    
    results <- bind_rows(results, reg_result)
  }
  return(results)
}

portfolio_1 <- portfolio_sort(merged_ret, period_1)
summary(portfolio_1)
portfolio_data <- merge(merged_ret, portfolio_1, by = c("PERMNO", "month"))

cw(portfolio_data,"portfolio_data.csv")
portfolio_data <- cr("portfolio_data.csv")

portfolio_returns <- portfolio_data %>%
  group_by(date, dvvix_ntile) %>%
  summarise(aer = mean(er),
            wer = sum((MC / sum(MC)) * er), .groups = "drop")

portfolio_returns <- portfolio_returns %>%
  group_by(date) %>%
  summarise(
    aer_diff = sum(aer[dvvix_ntile == 1]) - sum(aer[dvvix_ntile == 5]),
    wer_diff = sum(wer[dvvix_ntile == 1]) - sum(wer[dvvix_ntile == 5]),
    .groups = "drop"
  ) %>%
  mutate(dvvix_ntile = 6) %>%
  rename(aer = aer_diff,
    wer = wer_diff) %>%
  bind_rows(portfolio_returns, .) %>%
  arrange(date, dvvix_ntile)

data_list <- list(portfolio_returns, dvvix, dvix, ff5, hxz)
weighted_r <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE), data_list)
cw(weighted_r,"weighted_r.csv")

return_stat <- data.frame() 
for(i in (1:6)){
  statistics <- portfolio_returns %>%
    filter(dvvix_ntile == i) %>%
    summarise(
      mean_ER = mean(aer, na.rm = TRUE),        
      sd_ER = sd(aer, na.rm = TRUE),            
      skewness_ER = skewness(aer, na.rm = TRUE), 
      kurtosis_ER = kurtosis(aer, na.rm = TRUE)  
    )
  return_stat <- bind_rows(return_stat,statistics)
}

rolling_reg <- function(data,reg_formula){
  assets_results <- data %>%
    group_by(dvvix_ntile) %>%
    group_modify(~ {
      model <- lm(reg_formula, data = .x)
      tidy(model)
    })
  
  reg_result <- assets_results %>%
    select(dvvix_ntile, term, estimate, p.value) %>%
    pivot_wider(
      names_from = term, 
      values_from = c(estimate, p.value), 
      names_sep = "_"
    )
  
  return(reg_result)
}
reg_result <- data.frame()
reg_result <- bind_rows(reg_result,
                        rolling_reg(weighted_r,aer ~ mktrf + dvix + dvvix),
                        rolling_reg(weighted_r,aer ~ mktrf + dvix + dvvix+ smb + hml),
                        rolling_reg(weighted_r,aer ~ mktrf + dvix + dvvix+ smb + hml+ cma + rmw),
                        rolling_reg(weighted_r,aer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg)
                        )


head(reg_result)
cw(reg_result,"reg.csv")
reg_result <- cr("reg.csv")

weighted_reg_result <- data.frame()
weighted_reg_result <- bind_rows(weighted_reg_result,
                        rolling_reg(weighted_r, wer ~ mktrf + dvix + dvvix),
                        rolling_reg(weighted_r, wer ~ mktrf + dvix + dvvix+ smb + hml),
                        rolling_reg(weighted_r, wer ~ mktrf + dvix + dvvix+ smb + hml+ cma + rmw),
                        rolling_reg(weighted_r, wer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg)
)
head(weighted_reg_result)
head(portfolio_returns)

double_sort <- function(data,sort1,sort2,reg_formula){
    assets_results <- data %>%
      group_by(sort1) %>%
      group_by(sort2) %>%
      group_modify(~ {
        model <- lm(reg_formula, data = .x)
        tidy(model)
      })
    
    reg_result <- assets_results %>%
      select(dvvix_ntile, term, estimate, p.value) %>%
      pivot_wider(
        names_from = term, 
        values_from = c(estimate, p.value), 
        names_sep = "_"
      )
    
    return(reg_result)
}
double_sort_result <- bind_rows(
  double_sort(weighted_r,dvvix_ntile,dvix_ntile,aer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg),
  double_sort(weighted_r,dvvix_ntile,dvix_ntile,wer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg),
  double_sort(weighted_r,dvvix_ntile,dvrp_ntile,aer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg),
  double_sort(weighted_r,dvvix_ntile,dvrp_ntile,wer ~ r_mkt + dvix + dvvix+ r_me+ r_ia+ r_roe+ r_eg)
)
head(double_sort_result)