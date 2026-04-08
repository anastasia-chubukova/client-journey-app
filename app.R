library(shiny)
library(readr)
library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(DT)
library(networkD3)
library(ggplot2)
library(purrr)
library(bslib)
library(waiter)
library(later)
library(shinymanager)


options(shiny.maxRequestSize = 500 * 1024^2)

guess_delim <- function(path) {
  first_line <- readLines(path, n = 1, warn = FALSE, encoding = "UTF-8")
  first_line <- iconv(first_line, from = "", to = "UTF-8", sub = "")
  if (is.na(first_line) || length(first_line) == 0) return(",")
  if (str_count(first_line, ";") > str_count(first_line, ",")) ";" else ","
}


credentials <- data.frame(
  user = c("client1", "client2", "admin"),
  password = c(
    Sys.getenv("CLIENT1_PASS"),
    Sys.getenv("CLIENT2_PASS"),
    Sys.getenv("ADMIN_PASS")
  ),
  admin = c(FALSE, FALSE, TRUE),
  stringsAsFactors = FALSE
)

read_input_data <- function(path) {
  ext <- tolower(tools::file_ext(path))
  
  df <- tryCatch({
    if (ext %in% c("csv", "txt")) {
      delim <- guess_delim(path)
      
      tryCatch(
        read_delim(
          path,
          delim = delim,
          show_col_types = FALSE,
          progress = FALSE,
          locale = locale(encoding = "UTF-8")
        ),
        error = function(e) {
          read_delim(
            path,
            delim = delim,
            show_col_types = FALSE,
            progress = FALSE,
            locale = locale(encoding = "CP1251")
          )
        }
      )
    } else if (ext %in% c("xlsx", "xls")) {
      sheets <- readxl::excel_sheets(path)
      
      if (length(sheets) == 0) {
        stop("У файлі Excel не знайдено жодного аркуша")
      }
      
      df_excel <- read_excel(path, sheet = sheets[1])
      
      if (nrow(df_excel) == 0) {
        stop(paste0("Перший аркуш Excel порожній: ", sheets[1]))
      }
      
      df_excel
    } else {
      stop("Непідтримуваний формат файлу")
    }
  }, error = function(e) {
    stop(paste("Помилка читання файлу:", e$message))
  })
  
  names(df) <- trimws(names(df))
  names(df) <- iconv(names(df), from = "", to = "UTF-8", sub = "")
  
  df %>%
    mutate(across(where(is.character), ~ iconv(.x, from = "", to = "UTF-8", sub = "")))
}

detect_client_id_column <- function(df) {
  nms <- names(df)
  nms_clean <- tolower(trimws(nms))
  
  candidates <- c(
    "client_id", "client id", "clientid",
    "customer_id", "customer id", "customerid",
    "id клієнта", "клієнт_id", "клиент_id", "id client"
  )
  
  hit <- nms[nms_clean %in% candidates]
  
  if (length(hit) == 0) return(NULL)
  hit[1]
}

safe_parse_date <- function(x) {
  x <- as.character(x)
  out <- suppressWarnings(ymd_hms(x, quiet = TRUE))
  if (all(is.na(out))) out <- suppressWarnings(dmy_hms(x, quiet = TRUE))
  if (all(is.na(out))) out <- suppressWarnings(ymd(x, quiet = TRUE))
  if (all(is.na(out))) out <- suppressWarnings(dmy(x, quiet = TRUE))
  as.POSIXct(out, tz = "UTC")
}

make_node <- function(df, mode = "brand_category") {
  df %>%
    mutate(
      brand = if ("brand" %in% names(.)) coalesce(as.character(brand), "") else "",
      product_name = if ("product_name" %in% names(.)) coalesce(as.character(product_name), "") else "",
      brand = trimws(brand),
      product_name = trimws(product_name),
      node = case_when(
        mode == "category" ~ ifelse(product_name == "", "Unknown category", product_name),
        mode == "brand" ~ ifelse(brand == "", "Unknown brand", brand),
        mode == "brand_category" & brand != "" & product_name != "" ~ paste(brand, product_name, sep = " | "),
        mode == "brand_category" & brand != "" & product_name == "" ~ brand,
        mode == "brand_category" & brand == "" & product_name != "" ~ product_name,
        TRUE ~ "Unknown"
      )
    )
}

detect_sum_column <- function(df) {
  candidates <- c("sum", "amount", "price", "item_sum", "sales", "value", "Сума", "сума", "Ціна", "ціна")
  found <- candidates[candidates %in% names(df)]
  if (length(found) == 0) return(NULL)
  found[1]
}

safe_parse_numeric <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  
  # прибираємо пробіли між тисячами та нерозривні пробіли
  x <- gsub("[[:space:]\u00A0]", "", x, perl = TRUE)
  
  # якщо є тільки кома — вважаємо її десятковим роздільником
  only_comma <- grepl(",", x) & !grepl("\\.", x)
  x[only_comma] <- sub(",", ".", x[only_comma], fixed = TRUE)
  
  # якщо є і кома, і крапка — прибираємо коми як роздільники тисяч
  both_sep <- grepl(",", x) & grepl("\\.", x)
  x[both_sep] <- gsub(",", "", x[both_sep], fixed = TRUE)
  
  suppressWarnings(as.numeric(x))
}

prepare_events <- function(df, node_mode = "brand_category", basket_mode = c("main_item", "all_items")) {
  basket_mode <- match.arg(basket_mode)
  sum_col <- detect_sum_column(df)
  
  df_prepared <- df %>%
    mutate(
      client_id = as.character(client_id),
      transaction_id = as.character(transaction_id),
      transaction_date = safe_parse_date(transaction_date)
    ) %>%
    filter(
      !is.na(client_id), trimws(client_id) != "",
      !is.na(transaction_id), trimws(transaction_id) != "",
      !is.na(transaction_date)
    ) %>%
    make_node(node_mode) %>%
    mutate(row_id_internal = row_number())
  
  if (basket_mode == "main_item") {
    if (is.null(sum_col)) {
      stop("Для режиму 'Головний товар' не знайдено колонку з ціною/сумою.")
    }
    
    return(
      df_prepared %>%
        mutate(item_sum = safe_parse_numeric(.data[[sum_col]])) %>%
        group_by(client_id, transaction_id, transaction_date) %>%
        summarise(
          basket_nodes = list(sort(unique(node))),
          basket_label = {
            valid <- which(!is.na(item_sum))
            if (length(valid) == 0) {
              sort(unique(node))[1]
            } else {
              idx <- valid[which.max(item_sum[valid])]
              node[idx]
            }
          },
          .groups = "drop"
        ) %>%
        mutate(basket_nodes = map(basket_label, ~ .x)) %>%
        arrange(client_id, transaction_date, transaction_id) %>%
        group_by(client_id) %>%
        mutate(step = row_number()) %>%
        ungroup()
    )
  }
  
  df_prepared %>%
    distinct(client_id, transaction_id, transaction_date, node) %>%
    arrange(client_id, transaction_date, transaction_id, node) %>%
    group_by(client_id, transaction_id, transaction_date) %>%
    summarise(
      basket_nodes = list(sort(unique(node))),
      basket_label = paste(sort(unique(node)), collapse = " + "),
      .groups = "drop"
    ) %>%
    arrange(client_id, transaction_date, transaction_id) %>%
    group_by(client_id) %>%
    mutate(step = row_number()) %>%
    ungroup()
}

build_transitions <- function(events, min_days = 0, max_days = 365) {
  build_event_links(events, min_days, max_days) %>%
    filter(next_valid) %>%
    transmute(
      client_id,
      transaction_id,
      transaction_date,
      basket_nodes,
      basket_label,
      next_basket,
      next_nodes,
      next_date,
      lag_days
    )
}

build_item_transitions <- function(transitions_df, keep_self_transitions = TRUE) {
  out <- transitions_df %>%
    rowwise() %>%
    mutate(
      pair_tbl = list(
        expand_grid(
          from = unlist(basket_nodes),
          to = unlist(next_nodes)
        )
      )
    ) %>%
    ungroup() %>%
    select(client_id, lag_days, pair_tbl) %>%
    unnest(pair_tbl) %>%
    filter(!is.na(from), !is.na(to))
  
  if (!keep_self_transitions) {
    out <- out %>% filter(from != to)
  }
  
  out
}

summarise_item_transitions <- function(item_transitions) {
  from_base <- item_transitions %>%
    distinct(client_id, from) %>%
    count(from, name = "from_clients")
  
  item_transitions %>%
    group_by(from, to) %>%
    summarise(
      clients = n_distinct(client_id),
      transitions_n = n(),
      avg_lag_days = round(mean(lag_days, na.rm = TRUE), 1),
      median_lag_days = round(median(lag_days, na.rm = TRUE), 1),
      .groups = "drop"
    ) %>%
    left_join(from_base, by = "from") %>%
    mutate(conversion_from_clients = round(100 * clients / from_clients, 2)) %>%
    arrange(desc(clients), desc(transitions_n))
}

build_step_paths <- function(events, max_steps = 6) {
  events %>%
    group_by(client_id) %>%
    arrange(transaction_date, transaction_id, .by_group = TRUE) %>%
    mutate(step = row_number()) %>%
    filter(step <= max_steps) %>%
    ungroup() %>%
    select(client_id, step, basket_label) %>%
    mutate(step_name = paste0("step_", step)) %>%
    select(-step) %>%
    pivot_wider(
      names_from = step_name,
      values_from = basket_label
    )
}



make_journey_sankey_data <- function(path_df, max_steps = 6, top_n = 100) {
  if (nrow(path_df) == 0) {
    return(list(
      nodes = data.frame(name = character()),
      links = data.frame()
    ))
  }
  
  step_cols <- paste0("step_", 1:max_steps)
  step_cols <- step_cols[step_cols %in% names(path_df)]
  
  all_links <- map_dfr(seq_len(length(step_cols) - 1), function(i) {
    from_col <- step_cols[i]
    to_col <- step_cols[i + 1]
    
    path_df %>%
      filter(!is.na(.data[[from_col]]), !is.na(.data[[to_col]])) %>%
      count(
        from = .data[[from_col]],
        to = .data[[to_col]],
        name = "value",
        sort = TRUE
      ) %>%
      mutate(
        from_node = paste0(from_col, " | ", from),
        to_node = paste0(to_col, " | ", to)
      )
  })
  
  all_links <- all_links %>%
    arrange(desc(value)) %>%
    slice_head(n = top_n)
  
  nodes <- data.frame(
    name = unique(c(all_links$from_node, all_links$to_node)),
    stringsAsFactors = FALSE
  )
  
  links <- all_links %>%
    mutate(
      source = match(from_node, nodes$name) - 1,
      target = match(to_node, nodes$name) - 1
    ) %>%
    select(source, target, value)
  
  list(nodes = nodes, links = links)
}

build_replenishment <- function(df, node_mode = "brand_category", min_days = 1, max_days = 365) {
  df %>%
    mutate(
      client_id = as.character(client_id),
      transaction_id = as.character(transaction_id),
      transaction_date = safe_parse_date(transaction_date)
    ) %>%
    filter(
      !is.na(client_id), trimws(client_id) != "",
      !is.na(transaction_id), trimws(transaction_id) != "",
      !is.na(transaction_date)
    ) %>%
    make_node(node_mode) %>%
    distinct(client_id, transaction_id, transaction_date, node) %>%
    arrange(client_id, node, transaction_date) %>%
    group_by(client_id, node) %>%
    mutate(
      next_same_date = lead(transaction_date),
      lag_days = as.numeric(difftime(next_same_date, transaction_date, units = "days"))
    ) %>%
    ungroup() %>%
    filter(!is.na(lag_days), lag_days >= min_days, lag_days <= max_days)
}

summarise_replenishment <- function(repl_df) {
  repl_df %>%
    group_by(node) %>%
    summarise(
      clients = n_distinct(client_id),
      repeats_n = n(),
      avg_repurchase_days = round(mean(lag_days, na.rm = TRUE), 1),
      median_repurchase_days = round(median(lag_days, na.rm = TRUE), 1),
      .groups = "drop"
    ) %>%
    arrange(desc(clients), desc(repeats_n))
}

build_data_audit <- function(df) {
  sum_col <- detect_sum_column(df)
  
  df2 <- df %>%
    mutate(
      client_id = as.character(client_id),
      transaction_id = as.character(transaction_id),
      transaction_date_raw = as.character(transaction_date),
      transaction_date_parsed = safe_parse_date(transaction_date),
      product_name = if ("product_name" %in% names(.)) as.character(product_name) else NA_character_,
      brand = if ("brand" %in% names(.)) as.character(brand) else NA_character_
    )
  
  if (!is.null(sum_col)) {
    df2 <- df2 %>%
      mutate(
        sum_raw = as.character(.data[[sum_col]]),
        sum_parsed = safe_parse_numeric(.data[[sum_col]])
      )
  } else {
    df2 <- df2 %>%
      mutate(
        sum_raw = NA_character_,
        sum_parsed = NA_real_
      )
  }
  
  records_n <- nrow(df2)
  clients_n <- n_distinct(df2$client_id[!is.na(df2$client_id) & trimws(df2$client_id) != ""])
  transactions_n <- n_distinct(df2$transaction_id[!is.na(df2$transaction_id) & trimws(df2$transaction_id) != ""])
  categories_n <- n_distinct(df2$product_name[!is.na(df2$product_name) & trimws(df2$product_name) != ""])
  brands_n <- n_distinct(df2$brand[!is.na(df2$brand) & trimws(df2$brand) != ""])
  
  valid_dates <- df2$transaction_date_parsed[!is.na(df2$transaction_date_parsed)]
  min_date <- if (length(valid_dates) > 0) min(valid_dates) else NA
  max_date <- if (length(valid_dates) > 0) max(valid_dates) else NA
  
  avg_records_per_client <- ifelse(clients_n > 0, round(records_n / clients_n, 2), NA)
  avg_transactions_per_client <- ifelse(clients_n > 0, round(transactions_n / clients_n, 2), NA)
  
  sum_found_text <- ifelse(is.null(sum_col), "Ні", paste0("Так: ", sum_col))
  
  sum_raw_missing_n <- if (!is.null(sum_col)) {
    sum(is.na(df2$sum_raw) | trimws(df2$sum_raw) == "")
  } else {
    NA_integer_
  }
  
  sum_parse_fail_n <- if (!is.null(sum_col)) {
    sum(
      !(is.na(df2$sum_raw) | trimws(df2$sum_raw) == "") &
        is.na(df2$sum_parsed)
    )
  } else {
    NA_integer_
  }
  
  valid_sum_values <- if (!is.null(sum_col)) df2$sum_parsed[!is.na(df2$sum_parsed)] else numeric(0)
  sum_min <- if (length(valid_sum_values) > 0) round(min(valid_sum_values), 2) else NA
  sum_max <- if (length(valid_sum_values) > 0) round(max(valid_sum_values), 2) else NA
  sum_mean <- if (length(valid_sum_values) > 0) round(mean(valid_sum_values), 2) else NA
  
  metrics <- tibble::tibble(
    Показник = c(
      "Кількість записів",
      "Кількість транзакцій",
      "Кількість клієнтів",
      "Кількість категорій",
      "Кількість брендів",
      "Мінімальна дата",
      "Максимальна дата",
      "Кількість валідних дат",
      "Середня кількість записів на клієнта",
      "Середня кількість транзакцій на клієнта",
      "Колонка суми знайдена",
      "Пропущено в колонці суми",
      "Не вдалося розпізнати як число",
      "Мінімальна сума",
      "Максимальна сума",
      "Середня сума"
    ),
    Значення = c(
      records_n,
      transactions_n,
      clients_n,
      categories_n,
      brands_n,
      ifelse(all(is.na(min_date)), NA, as.character(as.Date(min_date))),
      ifelse(all(is.na(max_date)), NA, as.character(as.Date(max_date))),
      length(valid_dates),
      avg_records_per_client,
      avg_transactions_per_client,
      sum_found_text,
      sum_raw_missing_n,
      sum_parse_fail_n,
      sum_min,
      sum_max,
      sum_mean
    )
  )
  
  missing_tbl <- tibble::tibble(
    Поле = c("client_id", "transaction_id", "transaction_date", "product_name", "Бренд"),
    Пропущено = c(
      sum(is.na(df2$client_id) | trimws(df2$client_id) == ""),
      sum(is.na(df2$transaction_id) | trimws(df2$transaction_id) == ""),
      sum(is.na(df2$transaction_date_raw) | trimws(df2$transaction_date_raw) == ""),
      sum(is.na(df2$product_name) | trimws(df2$product_name) == ""),
      sum(is.na(df2$brand) | trimws(df2$brand) == "")
    ),
    `Унікальних значень` = c(
      n_distinct(df2$client_id[!is.na(df2$client_id) & trimws(df2$client_id) != ""]),
      n_distinct(df2$transaction_id[!is.na(df2$transaction_id) & trimws(df2$transaction_id) != ""]),
      n_distinct(df2$transaction_date_raw[!is.na(df2$transaction_date_raw) & trimws(df2$transaction_date_raw) != ""]),
      n_distinct(df2$product_name[!is.na(df2$product_name) & trimws(df2$product_name) != ""]),
      n_distinct(df2$brand[!is.na(df2$brand) & trimws(df2$brand) != ""])
    )
  ) %>%
    mutate(`% пропусків` = round(100 * Пропущено / records_n, 2))
  
  if (!is.null(sum_col)) {
    sum_missing_row <- tibble::tibble(
      Поле = sum_col,
      Пропущено = sum(is.na(df2$sum_raw) | trimws(df2$sum_raw) == ""),
      `Унікальних значень` = n_distinct(df2$sum_raw[!is.na(df2$sum_raw) & trimws(df2$sum_raw) != ""]),
      `% пропусків` = round(100 * sum(is.na(df2$sum_raw) | trimws(df2$sum_raw) == "") / records_n, 2)
    )
    
    missing_tbl <- bind_rows(missing_tbl, sum_missing_row)
  }
  
  client_activity <- df2 %>%
    filter(
      !is.na(client_id), trimws(client_id) != "",
      !is.na(transaction_id), trimws(transaction_id) != ""
    ) %>%
    distinct(client_id, transaction_id) %>%
    count(client_id, name = "transactions_per_client")
  
  client_activity_summary <- tibble::tibble(
    Показник = c(
      "Клієнтів з 1 транзакцією",
      "Клієнтів з 2 транзакціями",
      "Клієнтів з 3-4 транзакціями",
      "Клієнтів з 5+ транзакціями"
    ),
    Значення = c(
      sum(client_activity$transactions_per_client == 1, na.rm = TRUE),
      sum(client_activity$transactions_per_client == 2, na.rm = TRUE),
      sum(client_activity$transactions_per_client >= 3 & client_activity$transactions_per_client <= 4, na.rm = TRUE),
      sum(client_activity$transactions_per_client >= 5, na.rm = TRUE)
    )
  )
  
  category_top <- df2 %>%
    filter(!is.na(product_name), trimws(product_name) != "") %>%
    count(product_name, sort = TRUE, name = "Кількість записів")
  
  brand_top <- df2 %>%
    filter(!is.na(brand), trimws(brand) != "") %>%
    count(brand, sort = TRUE, name = "Кількість записів")
  
  sum_parse_examples <- if (!is.null(sum_col)) {
    df2 %>%
      filter(
        !(is.na(sum_raw) | trimws(sum_raw) == ""),
        is.na(sum_parsed)
      ) %>%
      distinct(Проблемне_значення = sum_raw) %>%
      slice_head(n = 20)
  } else {
    tibble::tibble(Проблемне_значення = character())
  }
  
  list(
    metrics = metrics,
    missing_tbl = missing_tbl,
    client_activity_summary = client_activity_summary,
    category_top = category_top,
    brand_top = brand_top,
    sum_parse_examples = sum_parse_examples
  )
}

make_recursive_focused_sankey_data <- function(
    transitions_summary,
    start_node,
    depth = 3,
    top_n_per_node = 3,
    min_clients_link = 1,
    max_label_chars = 32
) {
  if (is.null(start_node) || is.na(start_node) || start_node == "") {
    return(list(
      nodes = data.frame(
        name = character(),
        full_name = character(),
        level = character(),
        stringsAsFactors = FALSE
      ),
      links = data.frame(
        source = integer(),
        target = integer(),
        value = numeric()
      )
    ))
  }
  
  pretty_basket_label <- function(x, max_chars = 32) {
    x <- as.character(x)
    x <- ifelse(
      stringr::str_detect(x, fixed(" + ")),
      paste0(stringr::str_split_fixed(x, fixed(" + "), 2)[, 1], " +..."),
      x
    )
    ifelse(nchar(x) > max_chars, paste0(substr(x, 1, max_chars - 3), "..."), x)
  }
  
  all_links <- list()
  current_nodes <- tibble::tibble(node = start_node, level_num = 0)
  visited_edges <- character()
  
  for (lvl in seq_len(depth)) {
    next_links <- purrr::map_dfr(current_nodes$node, function(curr_node) {
      transitions_summary %>%
        dplyr::filter(from == curr_node, clients >= min_clients_link) %>%
        dplyr::arrange(desc(clients), desc(transitions_n)) %>%
        dplyr::slice_head(n = top_n_per_node) %>%
        dplyr::transmute(
          from_node = from,
          to_node = to,
          value = clients,
          from_level = lvl - 1,
          to_level = lvl
        )
    })
    
    if (nrow(next_links) == 0) break
    
    next_links <- next_links %>%
      dplyr::mutate(edge_id = paste(from_node, to_node, from_level, to_level, sep = "|||")) %>%
      dplyr::filter(!edge_id %in% visited_edges)
    
    if (nrow(next_links) == 0) break
    
    visited_edges <- c(visited_edges, next_links$edge_id)
    all_links[[lvl]] <- next_links
    current_nodes <- next_links %>%
      dplyr::distinct(node = to_node, level_num = to_level)
  }
  
  all_links_df <- dplyr::bind_rows(all_links)
  
  if (nrow(all_links_df) == 0) {
    return(list(
      nodes = data.frame(
        name = character(),
        full_name = character(),
        level = character(),
        stringsAsFactors = FALSE
      ),
      links = data.frame(
        source = integer(),
        target = integer(),
        value = numeric()
      )
    ))
  }
  
  node_levels_from <- all_links_df %>%
    dplyr::distinct(full_name = from_node, level_num = from_level)
  
  node_levels_to <- all_links_df %>%
    dplyr::distinct(full_name = to_node, level_num = to_level)
  
  node_levels <- dplyr::bind_rows(node_levels_from, node_levels_to) %>%
    dplyr::distinct() %>%
    dplyr::arrange(level_num, full_name)
  
  nodes <- node_levels %>%
    dplyr::mutate(
      node_key = paste(level_num, full_name, sep = "|||"),
      name = pretty_basket_label(full_name, max_chars = max_label_chars),
      level = paste0("level_", level_num)
    ) %>%
    dplyr::select(node_key, full_name, name, level)
  
  links <- all_links_df %>%
    dplyr::mutate(
      source_key = paste(from_level, from_node, sep = "|||"),
      target_key = paste(to_level, to_node, sep = "|||"),
      source = match(source_key, nodes$node_key) - 1,
      target = match(target_key, nodes$node_key) - 1
    ) %>%
    dplyr::select(source, target, value)
  
  list(
    nodes = as.data.frame(nodes),
    links = as.data.frame(links)
  )
}

split_node_fields <- function(x, node_mode) {
  x <- as.character(x)
  
  if (node_mode == "brand_category") {
    parts <- stringr::str_split_fixed(x, fixed(" | "), 2)
    tibble::tibble(
      node_label = x,
      brand = trimws(parts[, 1]),
      category = trimws(parts[, 2])
    )
  } else if (node_mode == "brand") {
    tibble::tibble(
      node_label = x,
      brand = x,
      category = NA_character_
    )
  } else {
    tibble::tibble(
      node_label = x,
      brand = NA_character_,
      category = x
    )
  }
}

safe_input_value <- function(x) {
  if (is.null(x) || length(x) == 0) return("")
  x <- as.character(x)[1]
  if (is.na(x) || trimws(x) == "") return("")
  trimws(x)
}

norm_text <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  stringr::str_to_lower(x)
}

make_cohort_sankey_data <- function(event_links,
                                    start_node,
                                    depth = 3,
                                    top_n_per_node = 10,
                                    min_clients_link = 1,
                                    max_label_chars = 32,
                                    adaptive_mode = TRUE,
                                    small_cohort_threshold = 100,
                                    small_branch_threshold = 15,
                                    top_n_large = 10,
                                    min_pct_large = 1,
                                    min_clients_large = 2) {
  
  empty_result <- list(
    nodes = data.frame(
      node_key = character(),
      full_name = character(),
      name = character(),
      level = character(),
      stringsAsFactors = FALSE
    ),
    links = data.frame(
      source = integer(),
      target = integer(),
      value = numeric(),
      clients = numeric(),
      pct_start = numeric(),
      
      step_from = integer(),
      step_to = integer()
    ),
    details = data.frame(
      step_from = integer(),
      step_to = integer(),
      from = character(),
      to = character(),
      clients = numeric(),
      pct_start = numeric(),
      
      stringsAsFactors = FALSE
    ),
    other_details = data.frame(
      step_from = integer(),
      step_to = integer(),
      from = character(),
      to_real = character(),
      clients = numeric(),
      pct_start = numeric(),
      
      grouped_into = character(),
      stringsAsFactors = FALSE
    ),
    cohort_size = 0,
    display_mode = "empty",
    display_message = "Немає даних для побудови Sankey."
  )
  
  if (is.null(start_node) || is.na(start_node) || trimws(start_node) == "") {
    return(empty_result)
  }
  
  pretty_label <- function(x, max_chars = 32) {
    x <- as.character(x)
    ifelse(
      nchar(x) > max_chars,
      paste0(substr(x, 1, max_chars - 3), "..."),
      x
    )
  }
  
  ev <- event_links %>%
    mutate(
      next_basket_label = ifelse(next_valid, next_basket, NA_character_)
    )
  
  start_events <- ev %>%
    rowwise() %>%
    mutate(has_start_node = start_node %in% unlist(basket_nodes)) %>%
    ungroup() %>%
    filter(has_start_node) %>%
    group_by(client_id) %>%
    slice_min(order_by = step, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    transmute(
      client_id,
      current_step = step,
      current_label = start_node,
      next_step = ifelse(next_valid, next_step, NA_real_),
      next_basket_label = ifelse(next_valid, next_basket, NA_character_)
    )
  
  if (nrow(start_events) == 0) {
    return(empty_result)
  }
  
  cohort_size <- n_distinct(start_events$client_id)
  all_links <- list()
  current_states <- start_events %>%
    mutate(level_num = 0)
  
  used_reduction <- FALSE
  
  all_other_details <- list()
  
  for (lvl in seq_len(depth)) {
    if (nrow(current_states) == 0) break
    
    next_candidates <- current_states %>%
      mutate(
        next_label_raw = ifelse(
          is.na(next_basket_label) | trimws(next_basket_label) == "",
          "Не повернулися",
          next_basket_label
        )
      )
    
    if (nrow(next_candidates) == 0) break
    
    level_links <- purrr::map_dfr(unique(next_candidates$current_label), function(src_label) {
      src_df <- next_candidates %>%
        filter(current_label == src_label)
      
      from_base_n <- n_distinct(src_df$client_id)
      
      raw_counts <- src_df %>%
        group_by(next_label_raw) %>%
        summarise(
          clients = n_distinct(client_id),
          .groups = "drop"
        ) %>%
        arrange(desc(clients), next_label_raw)
      
      if (nrow(raw_counts) == 0) {
        return(NULL)
      }
      
      no_return_tbl <- raw_counts %>%
        filter(next_label_raw == "Не повернулися")
      
      real_targets <- raw_counts %>%
        filter(next_label_raw != "Не повернулися")
      
      selected_real <- NULL
      other_tbl <- NULL
      other_details_tbl <- NULL
      
      auto_reduce <- adaptive_mode &&
        (from_base_n > small_cohort_threshold || nrow(real_targets) > small_branch_threshold)
      
      if (!auto_reduce) {
        selected_real <- real_targets %>%
          filter(clients >= min_clients_link) %>%
          arrange(desc(clients), next_label_raw)
      } else {
        used_reduction <<- TRUE
        
        # 1. Завжди беремо top N найбільших переходів
        selected_real <- real_targets %>%
          arrange(desc(clients), next_label_raw) %>%
          slice_head(n = top_n_large)
        
        # 2. Усе інше йде в "Інші переходи"
        other_real <- real_targets %>%
          filter(!(next_label_raw %in% selected_real$next_label_raw))
        
        other_clients_sum <- sum(other_real$clients, na.rm = TRUE)
        other_details_tbl <- NULL
        other_tbl <- NULL
        
        # 3. Створюємо "Інші переходи" тільки якщо там справді є що агрегувати
        if (nrow(other_real) > 0 && other_clients_sum >= min_clients_large) {
          other_tbl <- tibble::tibble(
            next_label_raw = "Інші переходи",
            clients = other_clients_sum
          )
          
          other_details_tbl <- other_real %>%
            mutate(
              step_from = lvl - 1,
              step_to = lvl,
              from = src_label,
              to_real = next_label_raw,
              
              pct_start = round(100 * clients / cohort_size, 1),
              grouped_into = "Інші переходи"
            ) %>%
            select(step_from, step_to, from, to_real, clients, pct_start, grouped_into)
        }
      }
      
      if (!is.null(other_details_tbl) && nrow(other_details_tbl) > 0) {
        all_other_details[[length(all_other_details) + 1]] <<- other_details_tbl
      }
      
      bind_rows(selected_real, other_tbl, no_return_tbl) %>%
        mutate(
          from = src_label,
          to = next_label_raw,
          from_level = lvl - 1,
          to_level = lvl,
          from_base = from_base_n,
          
          pct_start = round(100 * clients / cohort_size, 1)
        ) %>%
        select(from, to, from_level, to_level, clients, from_base, pct_start)
    })
    
    if (nrow(level_links) == 0) break
    
    all_links[[lvl]] <- level_links
    
    continue_targets <- level_links %>%
      filter(!(to %in% c("Не повернулися", "Інші переходи")))
    
    if (nrow(continue_targets) == 0) break
    
    next_states <- purrr::map_dfr(seq_len(nrow(continue_targets)), function(i) {
      src_label_i <- continue_targets$from[i]
      to_label_i <- continue_targets$to[i]
      
      src_clients <- next_candidates %>%
        filter(current_label == src_label_i) %>%
        filter(next_label_raw == to_label_i) %>%
        distinct(client_id, next_step)
      
      if (nrow(src_clients) == 0) return(NULL)
      
      ev_next <- ev %>%
        transmute(
          client_id,
          reached_step = step,
          current_label = basket_label,
          next_step_new = ifelse(next_valid, next_step, NA_real_),
          next_basket_label_new = ifelse(next_valid, next_basket, NA_character_)
        )
      
      src_clients2 <- src_clients %>%
        transmute(
          client_id,
          reached_step = next_step
        )
      
      ev_next %>%
        inner_join(src_clients2, by = c("client_id", "reached_step")) %>%
        transmute(
          client_id,
          current_step = reached_step,
          current_label,
          next_step = next_step_new,
          next_basket_label = next_basket_label_new,
          level_num = continue_targets$to_level[i]
        ) %>%
        distinct()
    })
    
    if (nrow(next_states) == 0) break
    
    current_states <- next_states
  }
  
  all_links_df <- bind_rows(all_links)
  
  if (nrow(all_links_df) == 0) {
    return(list(
      nodes = data.frame(
        node_key = character(),
        full_name = character(),
        name = character(),
        level = character(),
        stringsAsFactors = FALSE
      ),
      links = data.frame(
        source = integer(),
        target = integer(),
        value = numeric(),
        clients = numeric(),
        pct_start = numeric(),
        
        step_from = integer(),
        step_to = integer()
      ),
      details = data.frame(
        step_from = integer(),
        step_to = integer(),
        from = character(),
        to = character(),
        clients = numeric(),
        pct_start = numeric(),
        
        stringsAsFactors = FALSE
      ),
      other_details = bind_rows(all_other_details),
      cohort_size = cohort_size,
      display_mode = "empty_after_filter",
      display_message = "Після застосування фільтрів переходів не залишилося."
    ))
  }
  
  node_levels_from <- all_links_df %>%
    distinct(full_name = from, level_num = from_level)
  
  node_levels_to <- all_links_df %>%
    distinct(full_name = to, level_num = to_level)
  
  nodes <- bind_rows(node_levels_from, node_levels_to) %>%
    distinct() %>%
    arrange(level_num, full_name) %>%
    mutate(
      node_key = paste(level_num, full_name, sep = "|||"),
      name = pretty_label(full_name, max_chars = max_label_chars),
      level = paste0("level_", level_num)
    ) %>%
    select(node_key, full_name, name, level)
  
  links <- all_links_df %>%
    mutate(
      source_key = paste(from_level, from, sep = "|||"),
      target_key = paste(to_level, to, sep = "|||"),
      source = match(source_key, nodes$node_key) - 1,
      target = match(target_key, nodes$node_key) - 1,
      value = clients
    ) %>%
    select(
      source, target, value,
      clients, pct_start, 
      step_from = from_level,
      step_to = to_level
    )
  
  details <- all_links_df %>%
    transmute(
      step_from = from_level,
      step_to = to_level,
      from,
      to,
      clients,
      pct_start
    ) %>%
    arrange(step_to, desc(clients), from, to)
  
  other_details <- bind_rows(all_other_details)
  
  display_mode <- if (used_reduction) "reduced" else "full"
  display_message <- if (used_reduction) {
    paste0(
      "Для цього вузла знайдено багато переходів. ",
      "Для читабельності показані лише основні гілки, решта об'єднана в 'Інші переходи'."
    )
  } else {
    "Для цього вузла показані всі переходи."
  }
  
  list(
    nodes = as.data.frame(nodes),
    links = as.data.frame(links),
    details = as.data.frame(details),
    other_details = as.data.frame(other_details),
    cohort_size = cohort_size,
    display_mode = display_mode,
    display_message = display_message
  )
}

build_cohort_client_journey <- function(event_links, start_node, depth = 3) {
  if (is.null(start_node) || is.na(start_node) || trimws(start_node) == "") {
    return(tibble::tibble())
  }
  
  ev <- event_links %>%
    mutate(
      basket_label = as.character(basket_label),
      next_basket = as.character(next_basket),
      next_basket_label = ifelse(next_valid, next_basket, NA_character_)
    )
  
  start_events <- ev %>%
    rowwise() %>%
    mutate(has_start_node = start_node %in% unlist(basket_nodes)) %>%
    ungroup() %>%
    filter(has_start_node) %>%
    group_by(client_id) %>%
    slice_min(order_by = step, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    transmute(
      client_id,
      level_num = 0L,
      step = step,
      label = start_node
    )
  
  if (nrow(start_events) == 0) {
    return(tibble::tibble())
  }
  
  all_levels <- list(start_events)
  current_states <- start_events
  
  for (lvl in seq_len(depth)) {
    next_states <- current_states %>%
      transmute(
        client_id,
        step = step
      ) %>%
      inner_join(
        ev %>%
          transmute(
            client_id,
            step,
            next_step = ifelse(next_valid, next_step, NA_real_),
            next_label = ifelse(next_valid, next_basket, "Не повернулися")
          ),
        by = c("client_id", "step")
      ) %>%
      filter(!is.na(next_label)) %>%
      transmute(
        client_id,
        level_num = lvl,
        step = next_step,
        label = next_label
      ) %>%
      distinct()
    
    if (nrow(next_states) == 0) break
    
    all_levels[[length(all_levels) + 1]] <- next_states
    
    current_states <- next_states %>%
      filter(label != "Не повернулися", !is.na(step))
    
    if (nrow(current_states) == 0) break
  }
  
  bind_rows(all_levels) %>%
    arrange(client_id, level_num)
}

build_cohort_transition_clients <- function(journey_long) {
  if (nrow(journey_long) == 0) {
    return(tibble::tibble())
  }
  
  step_from <- journey_long %>%
    transmute(
      client_id,
      level_num,
      from = label
    )
  
  step_to <- journey_long %>%
    transmute(
      client_id,
      level_num = level_num - 1L,
      to = label
    )
  
  step_from %>%
    inner_join(step_to, by = c("client_id", "level_num")) %>%
    transmute(
      client_id,
      step_from = level_num,
      step_to = level_num + 1L,
      from,
      to
    ) %>%
    distinct()
}

build_cohort_route_clients <- function(journey_long) {
  if (nrow(journey_long) == 0) {
    return(tibble::tibble())
  }
  
  journey_long %>%
    mutate(level_col = paste0("L", level_num)) %>%
    select(client_id, level_col, label) %>%
    pivot_wider(names_from = level_col, values_from = label) %>%
    rowwise() %>%
    mutate(
      route = paste(na.omit(c_across(starts_with("L"))), collapse = " → "),
      route_depth = sum(!is.na(c_across(starts_with("L")))) - 1
    ) %>%
    ungroup()
}

build_client_profile_tbl <- function(df) {
  sum_col <- detect_sum_column(df)
  
  df2 <- df %>%
    mutate(
      client_id = as.character(client_id),
      transaction_id = as.character(transaction_id),
      transaction_date = safe_parse_date(transaction_date)
    ) %>%
    filter(!is.na(client_id), trimws(client_id) != "")
  
  tx_tbl <- df2 %>%
    filter(!is.na(transaction_id), trimws(transaction_id) != "") %>%
    distinct(client_id, transaction_id, transaction_date)
  
  base_tbl <- tx_tbl %>%
    group_by(client_id) %>%
    summarise(
      transactions_n = n_distinct(transaction_id),
      first_tx_date = min(transaction_date, na.rm = TRUE),
      last_tx_date = max(transaction_date, na.rm = TRUE),
      .groups = "drop"
    )
  
  if (!is.null(sum_col)) {
    money_tbl <- df2 %>%
      mutate(item_sum = safe_parse_numeric(.data[[sum_col]])) %>%
      group_by(client_id) %>%
      summarise(
        total_sum = round(sum(item_sum, na.rm = TRUE), 2),
        avg_item_sum = round(mean(item_sum, na.rm = TRUE), 2),
        .groups = "drop"
      )
    
    base_tbl <- base_tbl %>%
      left_join(money_tbl, by = "client_id")
  } else {
    base_tbl <- base_tbl %>%
      mutate(
        total_sum = NA_real_,
        avg_item_sum = NA_real_
      )
  }
  
  base_tbl
}

build_selected_clients_kpi <- function(tbl) {
  if (nrow(tbl) == 0) {
    return(tibble::tibble(
      Показник = c("Кількість клієнтів", "Остання активність"),
      Значення = c(0, NA)
    ))
  }
  
  tibble::tibble(
    Показник = c("Кількість клієнтів", "Остання активність"),
    Значення = c(
      n_distinct(tbl$client_id),
      as.character(max(tbl$last_tx_date, na.rm = TRUE))
    )
  )
}

build_cohort_sankey_kpis <- function(sk) {
  if (is.null(sk) || is.null(sk$details) || nrow(sk$details) == 0) {
    return(tibble::tibble(
      Показник = c(
        "Розмір стартової когорти",
        "Найпопулярніший 1-й наступний кошик",
        "Частка, що не повернулися після старту"
      ),
      Значення = c("-", "-", "-")
    ))
  }
  
  first_step <- sk$details %>%
    filter(step_from == 0, step_to == 1)
  
  best_next <- first_step %>%
    filter(!(to %in% c("Не повернулися", "Інші кошики"))) %>%
    arrange(desc(clients)) %>%
    slice(1)
  
  no_return_clients <- first_step %>%
    filter(to == "Не повернулися") %>%
    summarise(val = sum(clients, na.rm = TRUE)) %>%
    pull(val)
  
  if (length(no_return_clients) == 0 || is.na(no_return_clients)) {
    no_return_clients <- 0
  }
  
  no_return_pct <- if (is.null(sk$cohort_size) || sk$cohort_size == 0) {
    0
  } else {
    round(100 * no_return_clients / sk$cohort_size, 1)
  }
  
  tibble::tibble(
    Показник = c(
      "Розмір стартової когорти",
      "Найпопулярніший 1-й наступний кошик",
      "Частка, що не повернулися після старту"
    ),
    Значення = c(
      sk$cohort_size,
      if (nrow(best_next) == 0) "-" else paste0(best_next$to, " (", best_next$pct_start, "%)"),
      paste0(no_return_clients, " клієнтів (", no_return_pct, "%)")
    )
  )
}



build_cohort_table <- function(df) {
  df2 <- df %>%
    mutate(
      client_id = as.character(client_id),
      transaction_id = as.character(transaction_id),
      transaction_date = safe_parse_date(transaction_date)
    ) %>%
    filter(
      !is.na(client_id), trimws(client_id) != "",
      !is.na(transaction_id), trimws(transaction_id) != "",
      !is.na(transaction_date)
    ) %>%
    distinct(client_id, transaction_id, transaction_date) %>%
    mutate(
      order_month = floor_date(as.Date(transaction_date), unit = "month")
    )
  
  first_purchase <- df2 %>%
    group_by(client_id) %>%
    summarise(
      cohort_month = min(order_month),
      .groups = "drop"
    )
  
  cohort_data <- df2 %>%
    left_join(first_purchase, by = "client_id") %>%
    mutate(
      cohort_index = interval(cohort_month, order_month) %/% months(1)
    ) %>%
    filter(cohort_index >= 0)
  
  cohort_size <- cohort_data %>%
    filter(cohort_index == 0) %>%
    group_by(cohort_month) %>%
    summarise(
      cohort_size = n_distinct(client_id),
      .groups = "drop"
    )
  
  retention_long <- cohort_data %>%
    group_by(cohort_month, cohort_index) %>%
    summarise(
      customers_n = n_distinct(client_id),
      .groups = "drop"
    ) %>%
    left_join(cohort_size, by = "cohort_month") %>%
    mutate(
      retention_pct = round(100 * customers_n / cohort_size, 1)
    ) %>%
    arrange(cohort_month, cohort_index)
  
  retention_wide <- retention_long %>%
    select(cohort_month, cohort_index, retention_pct) %>%
    mutate(cohort_index = paste0("M", cohort_index)) %>%
    pivot_wider(
      names_from = cohort_index,
      values_from = retention_pct
    ) %>%
    arrange(cohort_month)
  
  list(
    long = retention_long,
    wide = retention_wide
  )
}

build_cohort_kpis <- function(retention_long) {
  if (nrow(retention_long) == 0) {
    return(tibble::tibble(
      Показник = character(),
      Значення = character()
    ))
  }
  
  m1 <- retention_long %>%
    filter(cohort_index == 1) %>%
    summarise(val = round(mean(retention_pct, na.rm = TRUE), 1)) %>%
    pull(val)
  
  m3 <- retention_long %>%
    filter(cohort_index == 3) %>%
    summarise(val = round(mean(retention_pct, na.rm = TRUE), 1)) %>%
    pull(val)
  
  best_cohort <- retention_long %>%
    filter(cohort_index == 1) %>%
    arrange(desc(retention_pct)) %>%
    slice(1)
  
  worst_cohort <- retention_long %>%
    filter(cohort_index == 1) %>%
    arrange(retention_pct) %>%
    slice(1)
  
  tibble::tibble(
    Показник = c(
      "Середній retention на 1-й місяць",
      "Середній retention на 3-й місяць",
      "Найкраща когорта на 1-й місяць",
      "Найслабша когорта на 1-й місяць"
    ),
    Значення = c(
      paste0(ifelse(is.na(m1), "-", m1), "%"),
      paste0(ifelse(is.na(m3), "-", m3), "%"),
      if (nrow(best_cohort) == 0) "-" else paste0(as.character(best_cohort$cohort_month), " (", best_cohort$retention_pct, "%)"),
      if (nrow(worst_cohort) == 0) "-" else paste0(as.character(worst_cohort$cohort_month), " (", worst_cohort$retention_pct, "%)")
    )
  )
}

plot_cohort_heatmap <- function(retention_long) {
  if (nrow(retention_long) == 0) return(NULL)
  
  ggplot(retention_long, aes(
    x = factor(cohort_index),
    y = factor(cohort_month),
    fill = retention_pct
  )) +
    geom_tile(color = "white") +
    geom_text(aes(label = paste0(retention_pct, "%")), size = 3) +
    scale_fill_gradient(low = "#E5F0FF", high = "#2563EB", na.value = "grey90") +
    labs(
      title = "Когортний аналіз утримання клієнтів",
      subtitle = "Кожен рядок — когорта за місяцем першої покупки",
      x = "Місяць життя когорти",
      y = "Когорта",
      fill = "Retention %"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      panel.grid = element_blank(),
      axis.text.x = element_text(angle = 0, vjust = 0.5),
      axis.text.y = element_text(size = 10)
    )
}

calc_sankey_height <- function(nodes_df,
                               min_height = 500,
                               max_height = 2200,
                               px_per_node = 36,
                               extra_padding = 120) {
  if (is.null(nodes_df) || nrow(nodes_df) == 0) return(min_height)
  
  max_nodes_in_level <- nodes_df %>%
    count(level, name = "n") %>%
    summarise(max_n = max(n, na.rm = TRUE)) %>%
    pull(max_n)
  
  h <- extra_padding + max_nodes_in_level * px_per_node
  h <- max(min_height, h)
  h <- min(max_height, h)
  
  h
}

build_event_links <- function(events, min_days = 0, max_days = 365) {
  events %>%
    arrange(client_id, transaction_date, transaction_id) %>%
    group_by(client_id) %>%
    mutate(
      step = row_number(),
      next_step = lead(step),
      next_basket = lead(basket_label),
      next_nodes = lead(basket_nodes),
      next_date = lead(transaction_date),
      lag_days = as.numeric(difftime(next_date, transaction_date, units = "days")),
      next_valid = !is.na(next_step) &
        !is.na(lag_days) &
        lag_days >= min_days &
        lag_days <= max_days
    ) %>%
    ungroup()
}

format_big_number <- function(x, digits = 0) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return("—")
  format(round(x, digits), big.mark = " ", decimal.mark = ",", scientific = FALSE)
}

format_money <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return("—")
  paste0(format_big_number(x, 2), " грн")
}

make_kpi_card <- function(title, value, subtitle, color = "#78aee0") {
  div(
    style = paste0(
      "
      height: 150px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background: #fcfaf7;
      border-radius: 20px;
      padding: 18px 20px;
      box-shadow: 0 6px 18px rgba(80, 110, 140, 0.06);
      border: 1px solid rgba(120, 174, 224, 0.18);
      border-left: 4px solid ", color, ";
      "
    ),
    div(
      style = "font-size: 15px; color: #5f6b76; font-weight: 500;",
      title
    ),
    div(
      style = paste0(
        "font-size: 32px; font-weight: 700; line-height: 1.1; color: ", color, ";"
      ),
      value
    ),
    div(
      style = "font-size: 13px; color: #8a8179; line-height: 1.4;",
      subtitle
    )
  )
}

make_section_header <- function(title, subtitle, description = NULL) {
  div(
    class = "section-header",
    div(class = "section-title", title),
    div(class = "section-subtitle", subtitle),
    if (!is.null(description)) {
      div(class = "section-description", description)
    }
  )
}

'%||%' <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}



app_theme <- bs_theme(
  version = 5,
  bg = "#f5efe8",
  fg = "#3f3a36",
  primary = "#78aee0",
  secondary = "#fc6736",
  base_font = font_google("Inter"),
  heading_font = font_google("Inter"),
  code_font = font_google("Inter")
)

ui <- secure_app(
  fluidPage(
    useWaiter(),
    theme = app_theme,
    tags$head(
      tags$style(HTML("
    body {
      background-color: #f2ebe3 !important;
      color: #3f3a36 !important;
    }

    .container-fluid {
      padding-left: 24px;
      padding-right: 24px;
      padding-top: 18px;
      padding-bottom: 18px;
    }

    h1, h2, h3, h4, h5 {
      color: #78aee0;
      letter-spacing: 1px;
    }

    .title-panel, .summary-title {
      color: #78aee0;
      font-weight: 700;
      letter-spacing: 1px;
    }

    .summary-title {
      font-size: 30px;
      line-height: 0.95;
      text-transform: uppercase;
      margin-bottom: 10px;
    }

    .summary-subtitle {
      color: #fc6736;
      font-size: 18px;
      letter-spacing: 3px;
      text-transform: uppercase;
      margin-bottom: 24px;
    }

    .block-title {
      color: #78aee0;
      font-size: 24px;
      font-weight: 700;
      letter-spacing: 0.5px;
      margin-top: 8px;
      margin-bottom: 14px;
    }

    .well, .wellPanel, .insight-box {
      background: #fffaf6 !important;
      border: 1px solid rgba(120, 174, 224, 0.18) !important;
      border-radius: 22px !important;
      box-shadow: 0 6px 18px rgba(120, 174, 224, 0.08);
      padding: 18px !important;
    }

    .sidebar {
      background: #f8f2eb;
    }

    .panel, .tab-pane {
      background: transparent !important;
    }

    
    .nav-tabs {
      border-bottom: none !important;
      margin-bottom: 22px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    
    .nav-tabs > li > a,
    .nav-tabs .nav-link {
      border: none !important;
      border-radius: 999px !important;
      background: #fbf7f2 !important;
      color: #6f97c3 !important;
      font-weight: 600;
      padding: 10px 18px !important;
      margin-right: 0 !important;
      transition: all 0.2s ease;
      box-shadow: none !important;
    }
    
    .nav-tabs > li > a:hover,
    .nav-tabs .nav-link:hover {
      background: #f3ebe3 !important;
      color: #fc6736 !important;
    }
    
    .nav-tabs > li.active > a,
    .nav-tabs > li.active > a:hover,
    .nav-tabs > li.active > a:focus,
    .nav-tabs .nav-link.active,
    .nav-tabs .nav-item.show .nav-link {
      background: #fc6736 !important;
      color: #ffffff !important;
      border: none !important;
      box-shadow: 0 6px 14px rgba(252, 103, 54, 0.18) !important;
    }
    
    .btn, .btn-default {
      background: #fc6736 !important;
      color: white !important;
      border: none !important;
      border-radius: 999px !important;
      padding: 10px 18px !important;
      font-weight: 600;
      box-shadow: 0 4px 14px rgba(252, 103, 54, 0.18);
    }

    .btn:hover, .btn-default:hover {
      background: #ef5b2b !important;
      color: white !important;
    }

    .form-control, .selectize-input, .selectize-control.single .selectize-input {
      border-radius: 16px !important;
      border: 1px solid rgba(120, 174, 224, 0.25) !important;
      background: #fffaf6 !important;
      min-height: 46px;
      box-shadow: none !important;
    }

    .selectize-dropdown, .selectize-dropdown-content {
      border-radius: 16px !important;
      border: 1px solid rgba(120, 174, 224, 0.20) !important;
      background: #fffaf6 !important;
    }

    .control-label {
      color: #fc6736 !important;
      font-weight: 700;
      letter-spacing: 0.5px;
      margin-bottom: 8px;
    }

    .irs--shiny .irs-bar {
      background: #78aee0 !important;
      border-top: 1px solid #78aee0 !important;
      border-bottom: 1px solid #78aee0 !important;
    }

    .irs--shiny .irs-single {
      background: #fc6736 !important;
    }

    .irs--shiny .irs-handle {
      border: 1px solid #78aee0 !important;
      background: white !important;
    }

    .datatables {
      background: #fffaf6 !important;
      border-radius: 18px !important;
      overflow: hidden;
    }

    table.dataTable thead th {
      background-color: #78aee0 !important;
      color: white !important;
      border-bottom: none !important;
    }

    table.dataTable tbody tr {
      background-color: #fffaf6 !important;
    }

    table.dataTable tbody tr:hover {
      background-color: #fdf3ec !important;
    }

    .dataTables_wrapper .dataTables_paginate .paginate_button.current {
      background: #78aee0 !important;
      color: white !important;
      border: none !important;
      border-radius: 999px !important;
    }

    .shiny-input-container {
      margin-bottom: 16px !important;
    }

    .main-title-custom {
      font-size: 72px;
      line-height: 0.88;
      font-weight: 700;
      color: #78aee0;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 20px;
    }

    .main-subtitle-custom {
      font-size: 18px;
      color: #fc6736;
      text-transform: uppercase;
      letter-spacing: 4px;
      margin-bottom: 30px;
    }

    .card-soft {
      background: #fffaf6;
      border-radius: 24px;
      padding: 20px;
      box-shadow: 0 8px 24px rgba(120, 174, 224, 0.08);
      border: 1px solid rgba(120, 174, 224, 0.16);
    }
    
    .table-card {
      background: #fffaf6;
      border-radius: 22px;
      padding: 14px;
      box-shadow: 0 8px 24px rgba(120, 174, 224, 0.08);
      border: 1px solid rgba(120, 174, 224, 0.16);
      margin-bottom: 18px;
    }
    
    .dataTables_wrapper {
      background: transparent !important;
    }
    
    .dataTables_wrapper .dataTables_filter,
    .dataTables_wrapper .dataTables_length,
    .dataTables_wrapper .dataTables_info,
    .dataTables_wrapper .dataTables_paginate {
      background: transparent !important;
    }
    
    table.dataTable {
      background: transparent !important;
      border-radius: 16px !important;
      overflow: hidden;
    }
    
    table.dataTable thead th {
      background: #78aee0 !important;
      color: white !important;
      border: none !important;
    }
    
    table.dataTable tbody tr {
      background-color: #fffaf6 !important;
    }
    
    table.dataTable tbody tr:nth-child(even) {
      background-color: #fdf3ec !important;
    }
    
    table.dataTable tbody tr:hover {
      background-color: #fde7dc !important;
    }
    
    .dataTables_wrapper .dataTables_filter input {
      background: #fffaf6 !important;
      border: 1px solid rgba(120, 174, 224, 0.25) !important;
      border-radius: 14px !important;
      padding: 6px 10px;
    }
    
    .dataTables_wrapper .dataTables_length select {
      background: #fffaf6 !important;
      border-radius: 12px !important;
      border: 1px solid rgba(120, 174, 224, 0.25) !important;
    }
    
    .dataTables_wrapper .dataTables_paginate .paginate_button {
      background: transparent !important;
      color: #78aee0 !important;
      border-radius: 999px !important;
      border: none !important;
    }
    
    .dataTables_wrapper .dataTables_paginate .paginate_button.current {
      background: #78aee0 !important;
      color: white !important;
    }
    
    table, .dataTable {
      font-family: 'Inter', sans-serif !important;
      font-size: 14px;
    }
    
    .info-card {
      background: #fcfaf7;
      border-radius: 22px;
      padding: 24px 28px;
      border: 1px solid rgba(120, 174, 224, 0.14);
      box-shadow: 0 6px 18px rgba(80, 110, 140, 0.05);
      margin-top: 18px;
    }
    
    .info-card h4 {
      font-size: 26px;
      color: #78aee0;
      margin-bottom: 12px;
      letter-spacing: -0.2px;
    }
    
    .info-card p,
    .info-card li {
      font-size: 16px;
      line-height: 1.65;
      color: #4f4741;
    }
    
    .info-card ul {
      margin-top: 10px;
      margin-bottom: 0;
      padding-left: 20px;
    }
  
    .section-header {
      margin-bottom: 22px;
    }
    
    .section-title {
      font-size: 40px;
      line-height: 1.05;
      font-weight: 700;
      color: #78aee0;
      letter-spacing: -0.5px;
      text-transform: none;
      margin-bottom: 6px;
    }
    
    .section-subtitle {
      font-size: 15px;
      color: #7b6f66;
      letter-spacing: 0.2px;
      text-transform: none;
      margin-top: 0;
    }
    
    .section-description {
      font-size: 15px;
      color: #5c524b;
      margin-top: 10px;
      max-width: 760px;
      line-height: 1.6;
    }
    
    
    .dataTable td {
      white-space: normal !important;
      word-break: break-word;
    }
    
    .dataTable th {
      white-space: normal !important;
    }
    
    /* 🔐 LOGIN PAGE */

    .auth-page {
      background: #f2ebe3 !important;
      font-family: 'Inter', sans-serif;
    }
    
    .auth-card {
      background: #fffaf6 !important;
      border-radius: 24px !important;
      padding: 28px !important;
      box-shadow: 0 10px 30px rgba(120, 174, 224, 0.12) !important;
      border: 1px solid rgba(120, 174, 224, 0.18) !important;
    }
    
    .auth-title {
      color: #78aee0 !important;
      font-size: 28px !important;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
    }
    
    .auth-subtitle {
      color: #fc6736 !important;
      letter-spacing: 2px;
      text-transform: uppercase;
    }
    
    .auth-input {
      border-radius: 16px !important;
      border: 1px solid rgba(120, 174, 224, 0.25) !important;
      background: #fffaf6 !important;
    }
    
    .auth-button {
      background: #fc6736 !important;
      border-radius: 999px !important;
      font-weight: 600;
    }
    
    .auth-button:hover {
      background: #ef5b2b !important;
    }

  "))
    ),
    titlePanel("Інтерактивний аналіз клієнтських маршрутів"),
    
    h3("Ви увійшли в систему"),
    verbatimTextOutput("current_user"),
    sidebarLayout(
      sidebarPanel(
        width = 3,
        class = "card-soft",
        fileInput("file", "Завантажте файл CSV/XLSX", accept = c(".csv", ".xlsx", ".xls")),
        radioButtons(
          "node_mode", "Рівень вузла",
          choices = c(
            "Бренд + категорія" = "brand_category",
            "Категорія" = "category",
            "Бренд" = "brand"
          ),
          selected = "brand_category"
        ),
        radioButtons(
          "basket_mode", "Логіка кошика",
          choices = c(
            "Головний товар кошика" = "main_item",
            "Усі товари кошика" = "all_items"
          ),
          selected = "main_item"
        ),
        numericInput(
          "min_days",
          "Мінімальний інтервал між покупками, днів",
          value = 1,
          min = 0
        ),
        
        checkboxInput(
          "use_max_days",
          "Обмежувати маршрут максимальним інтервалом між покупками",
          value = FALSE
        ),
        
        conditionalPanel(
          condition = "input.use_max_days == true",
          numericInput(
            "max_days",
            "Вважати покупки пов’язаними, якщо між ними не більше ніж, днів",
            value = 365,
            min = 1
          )
        ),
        
        div(
          class = "info-card",
          style = "margin-top: 10px; padding: 12px 14px;",
          HTML(
            "Якщо інтервал між покупками більший за вказане значення, такий перехід
     не включається в маршрут. Це допомагає відділити пов’язані покупки
     від далеких повторних покупок або нового циклу поведінки."
          )
        ),
        #numericInput("max_steps", "Макс. кроків у маршруті", 6, min = 2, max = 10),
        
        hr()
      ),
      mainPanel(
        width = 9,
        tabsetPanel(
          tabPanel(
            "Загальний огляд",
            br(),
            make_section_header(
              "Огляд поведінки клієнтів",
              "Загальна картина клієнтської бази"
            ),
            
            div(
              style = "display: flex; gap: 16px; margin-bottom: 16px;",
              
              div(style = "flex:1;", uiOutput("kpi_clients")),
              div(style = "flex:1;", uiOutput("kpi_transactions")),
              div(style = "flex:1;", uiOutput("kpi_return_rate")),
              div(style = "flex:1;", uiOutput("kpi_one_time"))
            ),
            
            div(
              style = "display: flex; gap: 16px;",
              
              div(style = "flex:1;", uiOutput("kpi_min_purchase_day")),
              div(style = "flex:1;", uiOutput("kpi_max_purchase_day")),
              div(style = "flex:1;", uiOutput("kpi_route_length")),
              div(style = "flex:1;", uiOutput("kpi_repurchase"))
            ),
            br(),
            div(
              class = "info-card",
              h4("Що це за інструмент"),
              p("Цей дашборд показує, як клієнти рухаються між покупками та як формується їхня поведінка з часом."),
              tags$ul(
                tags$li("які покупки клієнти роблять після першої"),
                tags$li("як часто вони повертаються"),
                tags$li("де відбувається втрата клієнтів"),
                tags$li("які маршрути формують сильніші сценарії поведінки")
              )
            )
          ),
          tabPanel(
            "Аудит даних",
            br(),
            make_section_header(
              "Аудит даних",
              
              "Перевірка повноти, коректності та структури даних перед аналізом. Допомагає виявити проблеми, які можуть впливати на результати."
            ),
            
            h4("Основні метрики"),
            div(
              id = "audit_metrics_tbl_block",
              class = "table-card",
              DTOutput("audit_metrics_tbl")
            ),
            br(),
            h4("Якість даних по полях"),
            div(
              id = "audit_missing_tbl_block",
              class = "table-card",
              DTOutput("audit_missing_tbl")
            ),
            br(),
            h4("Активність клієнтів"),
            div(
              id = "audit_client_activity_summary_tbl_block",
              class = "table-card",
              DTOutput("audit_client_activity_summary_tbl")
            ),
            br(),
            h4("Топ категорій"),
            div(
              class = "table-card",
              DTOutput("audit_category_top_tbl")
            ),
            br(),
            h4("Топ брендів"),
            div(
              class = "table-card",
              DTOutput("audit_brand_top_tbl")
            ),
            br(),
            h4("Проблемні значення в колонці суми"),
            div(
              class = "table-card",
              uiOutput("audit_sum_parse_examples")
            )),
          
          tabPanel(
            "Шлях після покупки",
            br(),
            make_section_header(
              "Шлях клієнтів",
              "Що відбувається після конкретної покупки",
              "Детальний аналіз маршруту клієнтів після обраного товару або категорії. Показує, куди рухається клієнт далі або де відбувається відтік."
            ),
            wellPanel(
              HTML("
    <b>Що показує цей графік:</b><br/>
    Ви обираєте стартовий вузол — товар, бренд або категорію.<br/>
    Далі графік показує, <b>який маршрут формують клієнти</b> після цієї стартової покупки.<br/><br/>
    
    Якщо частина клієнтів не зробила наступної покупки, це показується як <b>'Не повернулися'</b>.<br/>
    Менш масові переходи можуть об'єднуватися в <b>'Інші переходи'</b>.
  ")
            ),
            br(),
            fluidRow(
              
              column(
                8,
                selectizeInput("cohort_sankey_start_node", "Оберіть стартовий вузол", choices = NULL)
              )
            ),
            br(),
            fluidRow(
              column(
                3,
                numericInput("cohort_depth", "Глибина маршруту", 3, min = 1, max = 6)
              ),
              
              column(
                3,
                numericInput("cohort_min_clients", "Мін. клієнтів у зв'язку", 1, min = 1, max = 100)
              ),
              column(
                3,
                numericInput("cohort_label_chars", "Довжина підпису", 30, min = 10, max = 80)
              )
            ),
            br(),
            
            h4("Короткі висновки"),
            div(
              class = "table-card",
              DTOutput("cohort_sankey_kpi_tbl")),
            br(),
            htmlOutput("cohort_sankey_mode_text"),
            br(),
            
            div(
              id = "cohort_sankey_block",
              uiOutput("cohort_sankey_plot_ui")
            ),
            br(),
            h4("Розшифровка переходів"),
            div(
              id = "cohort_details_block",
              class = "table-card",
              DTOutput("cohort_sankey_details_tbl")
            ),
            br(),
            h4("Розшифровка 'Інші переходи'"),
            div(
              class = "table-card",
              uiOutput("cohort_sankey_other_details_tbl")
            ),
            br(),
            h4("Повні назви вузлів"),
            div(
              class = "table-card",
              DTOutput("cohort_sankey_nodes_tbl")),
            br(),
            hr(),
            h4("Клієнти за маршрутом / переходом"),
            radioButtons(
              "cohort_clients_mode",
              "Режим деталізації",
              choices = c(
                "Клієнти за переходом" = "transition",
                "Клієнти за повним маршрутом" = "route"
              ),
              selected = "transition",
              inline = TRUE
            ),
            conditionalPanel(
              condition = "input.cohort_clients_mode == 'transition'",
              selectizeInput(
                "cohort_transition_pick",
                "Оберіть перехід",
                choices = NULL,
                options = list(placeholder = "Оберіть перехід зі списку")
              )
            ),
            conditionalPanel(
              condition = "input.cohort_clients_mode == 'route'",
              selectizeInput(
                "cohort_route_pick",
                "Оберіть повний маршрут",
                choices = NULL,
                options = list(placeholder = "Оберіть маршрут зі списку")
              )
            ),
            br(),
            h4("Ключові показники вибраної групи клієнтів"),
            div(
              class = "table-card",
              DTOutput("cohort_selected_clients_kpi_tbl")),
            br(),
            h4("Список клієнтів"),
            div(
              class = "table-card",
              DTOutput("cohort_selected_clients_tbl")),
            br(),
            downloadButton("download_cohort_selected_clients", "Завантажити CSV")
          ),
          tabPanel(
            "Переходи",
            br(),
            make_section_header(
              "Переходи між покупками",
              "Аналіз зв'язків між товарами та категоріями."
            ),
            br(),
            selectizeInput(
              "filter_node",
              "Фільтр по вузлу",
              choices = NULL,
              selected = "",
              multiple = FALSE,
              options = list(
                placeholder = "Усі вузли",
                maxOptions = 1000,
                create = FALSE
              )
            ),
            br(),
            
            div(
              class = "table-card",
              DTOutput("from_tbl")
            )
          ),
          
          tabPanel(
            "Маршрути клієнтів",
            br(),
            make_section_header(
              "Маршрути клієнтів",
              "Як рухається клієнт між покупками",
              "Візуалізація повних шляхів клієнтів у вигляді Sankey-діаграми. Допомагає зрозуміти основні сценарії поведінки."
            ),
            fluidRow(
              
              column(
                8,
                selectizeInput("sankey_start_node", "Оберіть вузол", choices = NULL)
              )
            ),
            
            
            br(),
            fluidRow(
              column(3, numericInput("journey_depth", "Глибина маршруту", 3, min = 1, max = 6)),
              column(3, numericInput("journey_top_n", "Гілок з кожного вузла", 3, min = 1, max = 8)),
              column(3, numericInput("journey_min_clients", "Мін. клієнтів у зв'язку", 1, min = 1, max = 100)),
              column(3, numericInput("journey_label_chars", "Довжина підпису", 22, min = 10, max = 50))
            ),
            br(),
            div(
              id = "journey_sankey_block",
              uiOutput("journey_sankey_block")
            ),
            div(
              class = "info-card",
              p(
                strong("Що показує цей Sankey: "),
                "цей графік будує маршрути від обраного вузла і показує, куди клієнти найчастіше переходять далі."
              ),
              p(
                strong("Чим він відрізняється від когортного Sankey: "),
                "тут можна досліджувати маршрут від конкретного вузла як стартової точки аналізу. У когортному Sankey на наступній вкладці фокус на розвитку вибраної когорти клієнтів у часі."
              )
            ),
            br(),
            h4("Повні назви вузлів"),
            div(
              class = "table-card",
              DTOutput("journey_nodes_tbl"))
          ),
          tabPanel("Повернення", 
                   br(),
                   make_section_header(
                     "Повернення клієнтів",
                     "Як швидко клієнти повертаються",
                     "Аналіз повторних покупок та часу між ними. Допомагає зрозуміти регулярність споживання та поведінку клієнтів."
                   ),
                   div(
                     id = "replenishment_tbl_block",
                     class = "table-card",
                     DTOutput("replenishment_tbl")
                   ))
          )
        )
      )
    ),
    head_auth = tags$head(
      tags$style(HTML("
    body {
      background: #f2ebe3 !important;
      font-family: 'Inter', sans-serif !important;
      color: #3f3a36 !important;
    }

    .auth-page {
      background: #f2ebe3 !important;
    }

    .auth-container {
      background: transparent !important;
    }

    .panel-auth,
    .auth-panel,
    .login-panel,
    .auth-card {
      background: #fffaf6 !important;
      border: 1px solid rgba(120, 174, 224, 0.18) !important;
      border-radius: 24px !important;
      box-shadow: 0 10px 30px rgba(120, 174, 224, 0.12) !important;
      padding: 28px !important;
    }

    .form-control {
      border-radius: 16px !important;
      border: 1px solid rgba(120, 174, 224, 0.25) !important;
      background: #fffaf6 !important;
      min-height: 46px !important;
      box-shadow: none !important;
      font-family: 'Inter', sans-serif !important;
    }

    .btn, .btn-default, button {
      background: #fc6736 !important;
      color: white !important;
      border: none !important;
      border-radius: 999px !important;
      padding: 10px 18px !important;
      font-weight: 600 !important;
      box-shadow: 0 4px 14px rgba(252, 103, 54, 0.18) !important;
    }

    .btn:hover, .btn-default:hover, button:hover {
      background: #ef5b2b !important;
      color: white !important;
    }

    label, .control-label {
      color: #fc6736 !important;
      font-weight: 700 !important;
      letter-spacing: 0.5px !important;
      margin-bottom: 8px !important;
      font-family: 'Inter', sans-serif !important;
    }

    h1, h2, h3, h4, h5 {
      color: #78aee0 !important;
      font-family: 'Inter', sans-serif !important;
    }

    .main-title-custom {
      font-size: 42px !important;
      line-height: 1.05 !important;
      font-weight: 700 !important;
      color: #78aee0 !important;
      text-transform: uppercase !important;
      letter-spacing: 1px !important;
      margin-bottom: 12px !important;
    }

    .main-subtitle-custom {
      font-size: 15px !important;
      color: #fc6736 !important;
      text-transform: uppercase !important;
      letter-spacing: 3px !important;
      margin-bottom: 10px !important;
    }

    .card-soft {
      background: #fffaf6 !important;
      border-radius: 24px !important;
      padding: 20px !important;
      box-shadow: 0 8px 24px rgba(120, 174, 224, 0.08) !important;
      border: 1px solid rgba(120, 174, 224, 0.16) !important;
    }

    .auth-subtitle {
      color: #7b6f66 !important;
      font-size: 14px !important;
      letter-spacing: 0.3px !important;
      text-transform: none !important;
      margin-top: 14px !important;
      text-align: center !important;
    }
  "))
    ),
    
    # 🔽 Текст і стиль
    tags_top = div(
      class = "card-soft",
      style = "text-align:center;",
      div(class = "main-title-custom", "Customer Journey Analytics"),
      div(class = "main-subtitle-custom", "Secure access")
    ),
    
    tags_bottom = div(
      class = "auth-subtitle",
      "Secure access"
    )
  )
  



server <- function(input, output, session) {
  res_auth <- secure_server(
    check_credentials = check_credentials(credentials)
  )
  updating_filters <- reactiveVal(FALSE)
  is_loading <- reactiveVal(FALSE)
  loaded_data <- reactiveVal(NULL)
  cleaned_data_val <- reactiveVal(NULL)
  is_global_loading <- reactiveVal(FALSE)
  
  events_data_val <- reactiveVal(NULL)
  event_links_val <- reactiveVal(NULL)
  transitions_summary_val <- reactiveVal(NULL)
  audit_data_val <- reactiveVal(NULL)
  
  output$current_user <- renderText({
    paste("Поточний користувач:", res_auth$user)
  })
  
  raw_data <- reactive({
    req(input$file)
    df <- read_input_data(input$file$datapath)
    validate(
      need(nrow(df) > 0, "Файл порожній")
    )
    df
  })
  
  cleaned_data <- reactive({
    req(cleaned_data_val())
    cleaned_data_val()
  })
  
  lag_filter_stats <- reactive({
    req(event_links())
    
    lnk <- event_links()
    
    total_with_next <- lnk %>%
      filter(!is.na(next_step), !is.na(lag_days)) %>%
      nrow()
    
    below_min <- lnk %>%
      filter(!is.na(next_step), !is.na(lag_days), lag_days < input$min_days) %>%
      nrow()
    
    above_max <- if (isTRUE(input$use_max_days)) {
      lnk %>%
        filter(!is.na(next_step), !is.na(lag_days), lag_days > input$max_days) %>%
        nrow()
    } else {
      0
    }
    
    kept <- total_with_next - below_min - above_max
    
    list(
      total_with_next = total_with_next,
      below_min = below_min,
      above_max = above_max,
      kept = kept
    )
  })
  
  prepare_cleaned_data <- function(df) {
    client_col <- detect_client_id_column(df)
    
    if (is.null(client_col)) {
      stop("❌ Не знайдено колонку client_id.")
    }
    
    # transaction_id
    if (!("transaction_id" %in% names(df))) {
      tx_candidates <- c("transaction_id", "transaction id", "check_id", "receipt_id", "order_id", "bill_id", "id транзакції")
      tx_hit <- names(df)[tolower(trimws(names(df))) %in% tx_candidates]
      if (length(tx_hit) > 0) {
        df$transaction_id <- df[[tx_hit[1]]]
      } else {
        stop("❌ Не знайдено колонку transaction_id.")
      }
    }
    
    # transaction_date
    if (!("transaction_date" %in% names(df))) {
      dt_candidates <- c("transaction_date", "transaction date", "date", "order_date", "check_date", "дата")
      dt_hit <- names(df)[tolower(trimws(names(df))) %in% dt_candidates]
      if (length(dt_hit) > 0) {
        df$transaction_date <- df[[dt_hit[1]]]
      } else {
        stop("❌ Не знайдено колонку transaction_date.")
      }
    }
    
    # brand
    if (!("brand" %in% names(df))) {
      brand_candidates <- c("Бренд", "бренд", "brand", "Brand")
      brand_hit <- names(df)[names(df) %in% brand_candidates]
      
      if (length(brand_hit) > 0) {
        df$brand <- df[[brand_hit[1]]]
      } else {
        df$brand <- NA_character_
      }
    }
    
    # category / product_name
    if (!("product_name" %in% names(df))) {
      cat_candidates <- c("product_name", "category", "категорія", "Категорія", "product", "товар", "послуга")
      cat_hit <- names(df)[tolower(trimws(names(df))) %in% tolower(cat_candidates)]
      
      if (length(cat_hit) > 0) {
        df$product_name <- df[[cat_hit[1]]]
      } else {
        df$product_name <- NA_character_
      }
    }
    
    # перевірка: має бути хоча б один вимір
    has_brand <- any(!is.na(df$brand) & trimws(as.character(df$brand)) != "")
    has_category <- any(!is.na(df$product_name) & trimws(as.character(df$product_name)) != "")
    
    if (!has_brand && !has_category) {
      stop("❌ У файлі немає ані колонки бренду, ані колонки категорії/товару.")
    }
    
    df %>%
      mutate(
        client_id = trimws(as.character(.data[[client_col]])),
        transaction_id = trimws(as.character(transaction_id)),
        transaction_date = as.character(transaction_date),
        brand = as.character(brand),
        product_name = as.character(product_name)
      )
  }
  
  show_global_loader <- function(text = "Завантажуємо та обробляємо дані...") {
    is_global_loading(TRUE)
    
    waiter_show(
      html = tagList(
        spin_fading_circles(),
        tags$div(
          style = "margin-top: 18px; font-size: 18px; color: #78aee0; font-weight: 600;",
          text
        )
      ),
      color = "rgba(242, 235, 227, 0.92)"
    )
  }
  
  hide_global_loader <- function() {
    is_global_loading(FALSE)
    waiter_hide()
  }
  
  show_block_loader <- function(id, text) {
    waiter_show(
      id = id,
      html = tagList(
        spin_fading_circles(),
        tags$div(
          style = "margin-top: 16px; color: #78aee0; font-weight: 600;",
          text
        )
      ),
      color = "rgba(255,250,246,0.92)"
    )
  }
  
  hide_block_loader <- function(id) {
    waiter_hide(id = id)
  }
  
  rebuild_analysis <- function() {
    req(cleaned_data())
    
    df_clean <- cleaned_data()
    
    ev <- prepare_events(
      df_clean,
      node_mode = input$node_mode,
      basket_mode = input$basket_mode
    )
    
    max_days_val <- if (isTRUE(input$use_max_days)) input$max_days else Inf
    
    lnk <- build_event_links(
      ev,
      min_days = input$min_days,
      max_days = max_days_val
    )
    
    tr_raw <- lnk %>%
      filter(next_valid) %>%
      transmute(
        client_id,
        transaction_id,
        transaction_date,
        basket_nodes,
        basket_label,
        next_basket,
        next_nodes,
        next_date,
        lag_days
      )
    
    it <- build_item_transitions(
      tr_raw,
      keep_self_transitions = TRUE
    )
    
    tr_sum <- summarise_item_transitions(it)
    aud <- build_data_audit(df_clean)
    
    events_data_val(ev)
    event_links_val(lnk)
    transitions_summary_val(tr_sum)
    audit_data_val(aud)
  }
  
  events_data <- reactive({
    req(events_data_val())
    events_data_val()
  })
  
  event_links <- reactive({
    req(event_links_val())
    event_links_val()
  })
  
  transitions_summary <- reactive({
    req(transitions_summary_val())
    transitions_summary_val()
  })
  
  audit_data <- reactive({
    req(audit_data_val())
    audit_data_val()
  })
  
  transitions_raw <- reactive({
    req(event_links())
    event_links() %>%
      filter(next_valid) %>%
      transmute(
        client_id,
        transaction_id,
        transaction_date,
        basket_nodes,
        basket_label,
        next_basket,
        next_nodes,
        next_date,
        lag_days
      )
  })
  
  item_transitions <- reactive({
    req(transitions_raw())
    build_item_transitions(transitions_raw())
  })
  
  transitions_summary <- reactive({
    req(transitions_summary_val())
    transitions_summary_val()
  })
  
  audit_data <- reactive({
    req(audit_data_val())
    audit_data_val()
  })
  
  filtered_transition_choices <- reactive({
    req(transitions_summary())
    
    all_nodes <- sort(unique(c(transitions_summary()$from, transitions_summary()$to)))
    all_nodes
  })
  
  observe({
    choices <- filtered_transition_choices()
    current_selected <- isolate(input$filter_node)
    
    selected_value <- if (!is.null(current_selected) &&
                          length(current_selected) > 0 &&
                          current_selected %in% choices) {
      current_selected
    } else {
      ""
    }
    
    freezeReactiveValue(input, "filter_node")
    updateSelectizeInput(
      session,
      "filter_node",
      choices = c("Усі" = "", choices),
      selected = selected_value,
      server = TRUE
    )
  })
  
  
  replenishment_data <- reactive({
    build_replenishment(
      cleaned_data(),
      node_mode = input$node_mode,
      min_days = max(1, input$min_days),
      max_days = input$max_days
    )
  })
  
  replenishment_summary <- reactive({
    summarise_replenishment(replenishment_data())
  })
  
  audit_data <- reactive({
    build_data_audit(cleaned_data())
  })
  
  cohort_data <- reactive({
    build_cohort_table(cleaned_data())
  })
  
  cohort_kpis <- reactive({
    build_cohort_kpis(cohort_data()$long)
  })
  
  
  
  filtered_sankey_choices <- reactive({
    req(transitions_summary())
    
    all_nodes <- sort(unique(c(transitions_summary()$from, transitions_summary()$to)))
    
    search_txt <- input$sankey_search
    if (is.null(search_txt)) search_txt <- ""
    search_txt <- trimws(tolower(search_txt))
    
    if (search_txt == "") {
      return(all_nodes)
    }
    
    all_nodes[stringr::str_detect(tolower(all_nodes), fixed(search_txt))]
  })
  
  filtered_cohort_sankey_choices <- reactive({
    req(transitions_summary())
    
    all_nodes <- sort(unique(c(transitions_summary()$from, transitions_summary()$to)))
    
    search_txt <- input$cohort_sankey_search
    if (is.null(search_txt)) search_txt <- ""
    search_txt <- trimws(tolower(search_txt))
    
    if (search_txt == "") {
      return(all_nodes)
    }
    
    all_nodes[stringr::str_detect(tolower(all_nodes), fixed(search_txt))]
  })
  
  observe({
    choices <- filtered_cohort_sankey_choices()
    current_selected <- isolate(input$cohort_sankey_start_node)
    
    selected_value <- if (!is.null(current_selected) &&
                          length(current_selected) > 0 &&
                          current_selected %in% choices) {
      current_selected
    } else if (length(choices) > 0) {
      choices[1]
    } else {
      character(0)
    }
    
    freezeReactiveValue(input, "cohort_sankey_start_node")
    updateSelectizeInput(
      session,
      "cohort_sankey_start_node",
      choices = choices,
      selected = selected_value,
      server = TRUE
    )
  })
  
  cohort_sankey_data <- reactive({
    req(input$cohort_sankey_start_node)
    
    top_n_val <- 10
    
    make_cohort_sankey_data(
      event_links = event_links(),
      start_node = input$cohort_sankey_start_node,
      depth = input$cohort_depth,
      top_n_per_node = top_n_val,
      min_clients_link = input$cohort_min_clients,
      max_label_chars = input$cohort_label_chars,
      adaptive_mode = TRUE,
      small_cohort_threshold = 150,
      small_branch_threshold = 20,
      top_n_large = top_n_val,
      min_pct_large = 1,
      min_clients_large = input$cohort_min_clients
    )
  })
  
  client_profile_tbl <- reactive({
    build_client_profile_tbl(cleaned_data())
  })
  
  cohort_journey_long <- reactive({
    req(input$cohort_sankey_start_node)
    
    build_cohort_client_journey(
      event_links = event_links(),
      start_node = input$cohort_sankey_start_node,
      depth = input$cohort_depth
    )
  })
  
  cohort_transition_clients_all <- reactive({
    jl <- cohort_journey_long()
    req(nrow(jl) > 0)
    
    build_cohort_transition_clients(jl)
  })
  
  cohort_route_clients_all <- reactive({
    jl <- cohort_journey_long()
    req(nrow(jl) > 0)
    
    build_cohort_route_clients(jl)
  })
  
  
  
  
  
  executive_tx_tbl <- reactive({
    df <- cleaned_data()
    sum_col <- detect_sum_column(df)
    
    df2 <- df %>%
      mutate(
        client_id = as.character(client_id),
        transaction_id = as.character(transaction_id),
        transaction_date = safe_parse_date(transaction_date)
      ) %>%
      filter(
        !is.na(client_id), trimws(client_id) != "",
        !is.na(transaction_id), trimws(transaction_id) != "",
        !is.na(transaction_date)
      )
    
    if (is.null(sum_col)) {
      return(
        df2 %>%
          distinct(client_id, transaction_id, transaction_date) %>%
          mutate(check_sum = NA_real_)
      )
    }
    
    df2 %>%
      mutate(item_sum = safe_parse_numeric(.data[[sum_col]])) %>%
      group_by(client_id, transaction_id, transaction_date) %>%
      summarise(
        check_sum = sum(item_sum, na.rm = TRUE),
        .groups = "drop"
      )
  })
  
  executive_kpis <- reactive({
    
    df <- cleaned_data()
    
    df2 <- df %>%
      mutate(
        client_id = trimws(as.character(client_id)),
        transaction_id = trimws(as.character(transaction_id)),
        transaction_date = safe_parse_date(transaction_date)
      ) %>%
      filter(
        !is.na(client_id), client_id != "",
        !is.na(transaction_id), transaction_id != ""
      )
    
    # транзакції
    transactions_n <- df2 %>%
      distinct(client_id, transaction_id) %>%
      nrow()
    
    # клієнти
    clients_n <- df2 %>%
      distinct(client_id) %>%
      nrow()
    
    # транзакції на клієнта
    client_activity <- df2 %>%
      distinct(client_id, transaction_id) %>%
      count(client_id, name = "transactions_per_client")
    
    avg_tx_per_client <- round(mean(client_activity$transactions_per_client), 2)
    
    # одноразові
    one_time_share <- round(
      100 * mean(client_activity$transactions_per_client == 1),
      1
    )
    
    # частка клієнтів з 2+ транзакціями
    return_rate <- round(
      100 * mean(client_activity$transactions_per_client >= 2),
      1
    )
    
    # середня довжина маршруту
    avg_route_length <- avg_tx_per_client
    
    # перший крок
    first_steps <- df2 %>%
      make_node("brand_category") %>%
      group_by(client_id) %>%
      arrange(transaction_date, transaction_id, .by_group = TRUE) %>%
      slice(1) %>%
      ungroup() %>%
      count(node, sort = TRUE)
    
    top_first_step <- ifelse(nrow(first_steps) > 0, first_steps$node[1], NA)
    
    # повторні покупки
    repl_df <- build_replenishment(df2)
    
    avg_repurchase_days <- ifelse(
      nrow(repl_df) > 0,
      round(mean(repl_df$lag_days, na.rm = TRUE), 1),
      NA
    )
    valid_dates <- df2$transaction_date[!is.na(df2$transaction_date)]
    
    min_date <- if (length(valid_dates) > 0) min(valid_dates) else NA
    min_purchase_day <- if (all(is.na(min_date))) NA else as.Date(min_date)
    
    max_date <- if (length(valid_dates) > 0) max(valid_dates) else NA
    max_purchase_day <- if (all(is.na(max_date))) NA else as.Date(max_date)
    list(
      clients_n = clients_n,
      transactions_n = transactions_n,
      avg_tx_per_client = avg_tx_per_client,
      min_purchase_day = min_purchase_day,
      max_purchase_day = max_purchase_day,
      return_rate = return_rate,
      one_time_share = one_time_share,
      avg_route_length = avg_route_length,
      top_first_step = top_first_step,
      avg_repurchase_days = avg_repurchase_days
    )
  })
  
  output$kpi_clients <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Кількість клієнтів",
      value = format_big_number(k$clients_n),
      subtitle = "Унікальні клієнти у завантаженому файлі",
      color = "#78aee0"
    )
  })
  
  output$kpi_min_purchase_day <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Перша покупка",
      value = ifelse(is.na(k$min_purchase_day), "-", format(k$min_purchase_day, "%d.%m.%Y")),
      subtitle = "Найраніша дата транзакцій у файлі",
      color = "#78aee0"
    )
  })
  
  output$kpi_max_purchase_day <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Остання покупка",
      value = ifelse(is.na(k$max_purchase_day), "-", format(k$max_purchase_day, "%d.%m.%Y")),
      subtitle = "Найпізніша дата транзакцій у файлі",
      color = "#78aee0"
    )
  })
  
  output$kpi_avg_check <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Середній чек",
      value = format_money(k$avg_check),
      subtitle = "Розрахунок на рівні транзакції",
      color = "#78aee0"
    )
  })
  
  output$kpi_transactions <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Кількість транзакцій",
      value = format_big_number(k$transactions_n),
      subtitle = "Унікальні транзакції",
      color = "#78aee0"
    )
  })
  
  output$kpi_avg_tx <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Середня кількість транзакцій на клієнта",
      value = k$avg_tx_per_client,
      subtitle = "Глибина взаємодії",
      color = "#78aee0"
    )
  })
  
  output$kpi_one_time <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Клієнти без повторної покупки",
      value = paste0(format_big_number(k$one_time_share, 1), "%"),
      subtitle = "Клієнти з 1 покупкою",
      color = "#78aee0"
    )
  })
  
  output$kpi_route_length <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Середня довжина маршруту",
      value = k$avg_route_length,
      subtitle = "Кількість кроків клієнта",
      color = "#78aee0"
    )
  })
  
  output$kpi_first_step <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Найчастіший перший крок",
      value = k$top_first_step,
      subtitle = "З чого починається клієнт",
      color = "#78aee0"
    )
  })
  
  output$kpi_repurchase <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Час до повторної покупки",
      value = ifelse(is.na(k$avg_repurchase_days), "-", paste0(k$avg_repurchase_days, " дн")),
      subtitle = "Середній інтервал між покупками",
      color = "#78aee0"
    )
  })
  
  output$kpi_return_rate <- renderUI({
    k <- executive_kpis()
    make_kpi_card(
      title = "Клієнти з повторними покупками",
      value = paste0(format_big_number(k$return_rate, 1), "%"),
      subtitle = "Клієнти з 2+ транзакціями",
      color = "#78aee0"
    )
  })
  
  output$kpi_top_path <- renderUI({
    k <- executive_kpis()
    
    subtitle_text <- if (is.na(k$top_path_clients)) {
      "Немає достатньо даних"
    } else {
      paste0("Найпоширеніший шлях, ", format_big_number(k$top_path_clients), " клієнтів")
    }
    
    make_kpi_card(
      title = "Найпопулярніший шлях",
      value = ifelse(
        nchar(k$top_path) > 45,
        paste0(substr(k$top_path, 1, 45), "..."),
        k$top_path
      ),
      subtitle = subtitle_text,
      color = "#8B5CF6"
    )
  })
  
  output$executive_cohort_plot <- renderPlot({
    req(cohort_data())
    req(nrow(cohort_data()$long) > 0)
    
    plot_cohort_heatmap(cohort_data()$long)
  })
  
  output$executive_insights <- renderUI({
    k <- executive_kpis()
    
    insight_1 <- paste0(
      "<b>1.</b> У файлі знайдено <b>", format_big_number(k$clients_n),
      "</b> клієнтів."
    )
    
    insight_2 <- if (is.na(k$avg_check)) {
      "<b>2.</b> Колонка суми не знайдена, тому середній чек поки не розраховано."
    } else {
      paste0(
        "<b>2.</b> Середній чек становить <b>", format_money(k$avg_check), "</b>."
      )
    }
    
    insight_3 <- paste0(
      "<b>3.</b> Частка клієнтів, які повернулися хоча б ще раз: <b>",
      format_big_number(k$return_rate, 1), "%</b>."
    )
    
    insight_4 <- if (is.na(k$top_path_clients) || identical(k$top_path, "—")) {
      "<b>4.</b> Основний маршрут поки не визначено."
    } else {
      paste0(
        "<b>4.</b> Найпоширеніший короткий маршрут: <b>",
        htmltools::htmlEscape(k$top_path),
        "</b>."
      )
    }
    
    tags$div(
      class = "insight-box",
      HTML(paste(insight_1, insight_2, insight_3, insight_4, sep = "<br><br>"))
    )
  })
  
  
  
  
  
  observe({
    tbl <- cohort_transition_clients_all()
    
    if (nrow(tbl) == 0) {
      updateSelectizeInput(session, "cohort_transition_pick", choices = character(0), selected = character(0), server = TRUE)
    } else {
      choices_tbl <- tbl %>%
        distinct(step_from, step_to, from, to) %>%
        arrange(step_to, from, desc(to)) %>%
        mutate(
          choice_label = paste0("[", step_from, " → ", step_to, "] ", from, " → ", to),
          choice_value = paste(step_from, step_to, from, to, sep = "|||")
        )
      
      updateSelectizeInput(
        session,
        "cohort_transition_pick",
        choices = stats::setNames(choices_tbl$choice_value, choices_tbl$choice_label),
        selected = if (nrow(choices_tbl) > 0) choices_tbl$choice_value[1] else character(0),
        server = TRUE
      )
    }
  })
  
  observe({
    tbl <- cohort_route_clients_all()
    
    if (nrow(tbl) == 0) {
      updateSelectizeInput(session, "cohort_route_pick", choices = character(0), selected = character(0), server = TRUE)
    } else {
      route_choices <- tbl %>%
        distinct(route, route_depth) %>%
        filter(!is.na(route), trimws(route) != "") %>%
        count(route, route_depth, name = "clients") %>%
        arrange(desc(clients), desc(route_depth), route) %>%
        mutate(
          choice_label = paste0(route, "  (", clients, " клієнтів)"),
          choice_value = route
        )
      
      updateSelectizeInput(
        session,
        "cohort_route_pick",
        choices = stats::setNames(route_choices$choice_value, route_choices$choice_label),
        selected = if (nrow(route_choices) > 0) route_choices$choice_value[1] else character(0),
        server = TRUE
      )
    }
  })
  
  cohort_selected_clients_tbl <- reactive({
    profiles <- client_profile_tbl()
    
    mode <- input$cohort_clients_mode %||% "transition"
    
    if (mode == "transition") {
      req(input$cohort_transition_pick)
      
      parts <- strsplit(input$cohort_transition_pick, "\\|\\|\\|")[[1]]
      req(length(parts) == 4)
      
      step_from_val <- as.integer(parts[1])
      step_to_val <- as.integer(parts[2])
      from_val <- parts[3]
      to_val <- parts[4]
      
      cohort_transition_clients_all() %>%
        filter(
          step_from == step_from_val,
          step_to == step_to_val,
          from == from_val,
          to == to_val
        ) %>%
        distinct(client_id) %>%
        left_join(profiles, by = "client_id") %>%
        arrange(desc(last_tx_date), desc(transactions_n), client_id)
      
    } else {
      req(input$cohort_route_pick)
      
      cohort_route_clients_all() %>%
        filter(route == input$cohort_route_pick) %>%
        distinct(client_id, route, route_depth) %>%
        left_join(profiles, by = "client_id") %>%
        arrange(desc(last_tx_date), desc(transactions_n), client_id)
    }
  })
  
  output$cohort_selected_clients_kpi_tbl <- renderDT({
    tbl <- cohort_selected_clients_tbl()
    
    kpi_tbl <- build_selected_clients_kpi(tbl) %>%
      rename(
        `Показник` = Показник,
        `Значення` = Значення
      )
    
    datatable(
      kpi_tbl,
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$cohort_selected_clients_tbl <- renderDT({
    tbl <- cohort_selected_clients_tbl()
    
    if ("first_tx_date" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(first_tx_date = as.Date(first_tx_date))
    }
    
    if ("last_tx_date" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(last_tx_date = as.Date(last_tx_date))
    }
    
    # 🔥 ПРИБИРАЄМО НЕПОТРІБНІ КОЛОНКИ
    tbl <- tbl %>%
      select(-any_of(c("total_sum", "avg_item_sum")))
    
    rename_map <- c(
      client_id = "Клієнт",
      route = "Маршрут",
      route_depth = "Глибина маршруту",
      transactions_n = "Кількість транзакцій",
      first_tx_date = "Дата першої покупки",
      last_tx_date = "Дата останньої покупки"
    )
    
    existing_map <- rename_map[names(rename_map) %in% names(tbl)]
    if (length(existing_map) > 0) {
      tbl <- tbl %>% rename(!!!setNames(names(existing_map), existing_map))
    }
    
    datatable(
      tbl,
      options = list(pageLength = 20, scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$download_cohort_selected_clients <- downloadHandler(
    filename = function() {
      paste0("cohort_clients_", Sys.Date(), ".csv")
    },
    content = function(file) {
      df <- cohort_selected_clients_tbl()
      
      readr::write_excel_csv(df, file)
    }
  )
  
  output$cohort_sankey_kpi_tbl <- renderDT({
    sk <- cohort_sankey_data()
    
    datatable(
      build_cohort_sankey_kpis(sk),
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$cohort_sankey_details_tbl <- renderDT({
    show_block_loader("cohort_details_block", "Готуємо деталізацію переходів...")
    on.exit(hide_block_loader("cohort_details_block"), add = TRUE)
    
    sk <- cohort_sankey_data()
    
    validate(
      need(!is.null(sk), "Немає даних для відображення."),
      need(nrow(sk$details) > 0, "Для обраного стартового вузла деталізація переходів відсутня.")
    )
    
    tbl <- sk$details
    
    if ("level_from" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(
          level_from = case_when(
            level_from == "level_0" ~ "Стартовий крок",
            level_from == "level_1" ~ "2-й крок",
            level_from == "level_2" ~ "3-й крок",
            level_from == "level_3" ~ "4-й крок",
            level_from == "level_4" ~ "5-й крок",
            level_from == "level_5" ~ "6-й крок",
            TRUE ~ level_from
          )
        )
    }
    
    if ("level_to" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(
          level_to = case_when(
            level_to == "level_0" ~ "Стартовий крок",
            level_to == "level_1" ~ "2-й крок",
            level_to == "level_2" ~ "3-й крок",
            level_to == "level_3" ~ "4-й крок",
            level_to == "level_4" ~ "5-й крок",
            level_to == "level_5" ~ "6-й крок",
            TRUE ~ level_to
          )
        )
    }
    
    rename_map <- c(
      step_from = "Стартовий рівень",
      step_to = "Рівень переходу",
      clients = "Клієнтів",
      transitions_n = "Кількість переходів",
      avg_lag_days = "Сер. лаг, днів",
      median_lag_days = "Медіанний лаг, днів",
      from_clients = "Клієнтів у стартовому вузлі",
      conversion_from_clients = "Конверсія, %",
      level_from = "Рівень старту",
      level_to = "Рівень переходу",
      source = "Стартовий вузол",
      target = "Наступний вузол",
      value = "Клієнтів",
      from = "Від вузла",
      to = "До вузла",
      pct_start = "Частка від старту, %"
    )
    
    existing_map <- rename_map[names(rename_map) %in% names(tbl)]
    if (length(existing_map) > 0) {
      tbl <- tbl %>% rename(!!!setNames(names(existing_map), existing_map))
    }
    
    tbl <- tbl %>% select(-any_of("grouped_into"))
    
    datatable(
      tbl,
      options = list(scrollX = TRUE, pageLength = 15),
      rownames = FALSE
    )
  })
  
  journey_sankey_data <- reactive({
    req(input$sankey_start_node)
    
    make_recursive_focused_sankey_data(
      transitions_summary = transitions_summary(),
      start_node = input$sankey_start_node,
      depth = input$journey_depth,
      top_n_per_node = input$journey_top_n,
      min_clients_link = input$journey_min_clients,
      max_label_chars = input$journey_label_chars
    )
  })
  
  # ----------------------------
  # STABLE FILTER LOGIC
  # ----------------------------
  
  
  
  filtered_transition_choices <- reactive({
    req(transitions_summary())
    
    all_nodes <- sort(unique(c(transitions_summary()$from, transitions_summary()$to)))
    
    search_txt <- input$transition_search
    if (is.null(search_txt)) search_txt <- ""
    search_txt <- trimws(tolower(search_txt))
    
    if (search_txt == "") {
      return(all_nodes)
    }
    
    all_nodes[stringr::str_detect(tolower(all_nodes), fixed(search_txt))]
  })
  
  observe({
    choices <- filtered_transition_choices()
    current_selected <- isolate(input$filter_node)
    
    selected_value <- if (!is.null(current_selected) &&
                          length(current_selected) > 0 &&
                          current_selected %in% choices) {
      current_selected
    } else {
      ""
    }
    
    freezeReactiveValue(input, "filter_node")
    updateSelectizeInput(
      session,
      "filter_node",
      choices = c("Усі" = "", choices),
      selected = selected_value,
      server = TRUE
    )
  })
  
  
  
  
  observe({
    choices <- filtered_sankey_choices()
    current_selected <- isolate(input$sankey_start_node)
    
    selected_value <- if (!is.null(current_selected) &&
                          length(current_selected) > 0 &&
                          current_selected %in% choices) {
      current_selected
    } else if (length(choices) > 0) {
      choices[1]
    } else {
      character(0)
    }
    
    freezeReactiveValue(input, "sankey_start_node")
    updateSelectizeInput(
      session,
      "sankey_start_node",
      choices = choices,
      selected = selected_value,
      server = TRUE
    )
  })
  
  output$from_tbl <- renderDT({
    req(transitions_summary())
    
    tbl <- transitions_summary()
    
    validate(
      need(nrow(tbl) > 0, "Немає переходів для побудови таблиці. Перевірте дані або лаги.")
    )
    
    if (!is.null(input$filter_node) && input$filter_node != "") {
      tbl <- tbl %>% filter(from == input$filter_node)
    }
    
    validate(
      need(nrow(tbl) > 0, "За поточним фільтром переходів нічого не знайдено.")
    )
    
    tbl <- tbl %>%
      rename(
        `Звідки` = from,
        `Куди` = to,
        `Клієнтів` = clients,
        `Кількість переходів` = transitions_n,
        `Сер. лаг, днів` = avg_lag_days,
        `Медіанний лаг, днів` = median_lag_days,
        `Клієнтів у стартовому вузлі` = from_clients,
        `Конверсія, %` = conversion_from_clients
      ) %>%
      arrange(desc(`Клієнтів`), desc(`Кількість переходів`))
    
    datatable(
      tbl,
      options = list(scrollX = TRUE, pageLength = 15),
      rownames = FALSE
    )
  })
  
  output$replenishment_tbl <- renderDT({
    
    show_block_loader("replenishment_tbl_block", "Розраховуємо повторні покупки...")
    on.exit(hide_block_loader("replenishment_tbl_block"), add = TRUE)
    
    req(replenishment_summary())
    
    tbl <- replenishment_summary() %>%
      rename(
        `Товар / категорія` = node,
        `Клієнтів з повторними покупками` = clients,
        `Кількість повторних покупок` = repeats_n,
        `Сер. час до повторної покупки, днів` = avg_repurchase_days,
        `Медіанний час до повторної покупки, днів` = median_repurchase_days
      )
    
    datatable(
      tbl,
      options = list(scrollX = TRUE, pageLength = 20),
      rownames = FALSE
    )
  })
  
  output$journey_sankey_plot <- renderSankeyNetwork({
    sk <- journey_sankey_data()
    
    validate(
      need(!is.null(sk), ""),
      need(nrow(sk$nodes) > 0, ""),
      need(nrow(sk$links) > 0, "")
    )
    
    colour_scale <- 'd3.scaleOrdinal()
    .domain(["level_0", "level_1", "level_2", "level_3", "level_4", "level_5", "level_6"])
    .range(["#2563EB", "#60A5FA", "#F59E0B", "#34D399", "#F472B6", "#A78BFA", "#9CA3AF"])'
    
    sankeyNetwork(
      Links = sk$links,
      Nodes = sk$nodes,
      Source = "source",
      Target = "target",
      Value = "value",
      NodeID = "name",
      NodeGroup = "level",
      fontSize = 12,
      nodeWidth = 30,
      nodePadding = 22,
      
      colourScale = colour_scale
    )
  })
  
  output$cohort_sankey_plot <- renderSankeyNetwork({
    sk <- cohort_sankey_data()
    
    validate(
      need(!is.null(sk), ""),
      need(nrow(sk$nodes) > 0, ""),
      need(nrow(sk$links) > 0, "")
    )
    
    colour_scale <- 'd3.scaleOrdinal()
  .domain(["level_0", "level_1", "level_2", "level_3", "level_4", "level_5", "level_6"])
  .range(["#2563EB", "#60A5FA", "#F59E0B", "#34D399", "#F472B6", "#A78BFA", "#9CA3AF"])'
    
    sankeyNetwork(
      Links = sk$links,
      Nodes = sk$nodes,
      Source = "source",
      Target = "target",
      Value = "value",
      NodeID = "name",
      NodeGroup = "level",
      fontSize = 12,
      nodeWidth = 30,
      nodePadding = 22,
      sinksRight = FALSE,
      colourScale = colour_scale
    )
  })
  
  
  
  output$journey_sankey_block <- renderUI({
    show_block_loader("journey_sankey_block", "Будуємо маршрут клієнтів...")
    on.exit(hide_block_loader("journey_sankey_block"), add = TRUE)
    
    sk <- journey_sankey_data()
    
    if (is.null(sk) || nrow(sk$nodes) == 0 || nrow(sk$links) == 0) {
      return(
        div(
          class = "info-card",
          style = "min-height: 220px; display:flex; align-items:center;",
          div(
            style = "width:100%;",
            h4("Маршрути для обраного вузла не знайдено"),
            p("За поточними параметрами для цього вузла немає достатньо переходів для побудови Sankey-діаграми."),
            tags$ul(
              tags$li("оберіть інший вузол"),
              tags$li("зменште мінімальну кількість клієнтів у зв'язку"),
              tags$li("збільшіть глибину маршруту або кількість гілок"),
              tags$li("перевірте фільтри та логіку кошика")
            )
          )
        )
      )
    }
    
    sankeyNetworkOutput("journey_sankey_plot", height = "850px")
  })
  
  output$cohort_sankey_nodes_tbl <- renderDT({
    sk <- cohort_sankey_data()
    
    validate(
      need(!is.null(sk), "Немає даних для відображення."),
      need(nrow(sk$nodes) > 0, "Для обраного стартового вузла список вузлів недоступний.")
    )
    
    tbl <- sk$nodes %>%
      select(level, name, full_name) %>%
      mutate(
        level = case_when(
          level == "level_0" ~ "Стартовий крок",
          level == "level_1" ~ "2-й крок",
          level == "level_2" ~ "3-й крок",
          level == "level_3" ~ "4-й крок",
          level == "level_4" ~ "5-й крок",
          level == "level_5" ~ "6-й крок",
          TRUE ~ level
        )
      ) %>%
      rename(
        `Рівень маршруту` = level,
        `Коротка назва` = name,
        `Повна назва вузла` = full_name
      )
    
    datatable(
      tbl,
      options = list(scrollX = TRUE, pageLength = 10),
      rownames = FALSE
    )
  })
  
  
  output$cohort_kpi_tbl <- renderDT({
    datatable(
      cohort_kpis(),
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$cohort_heatmap <- renderPlot({
    req(nrow(cohort_data()$long) > 0)
    plot_cohort_heatmap(cohort_data()$long)
  })
  
  output$cohort_tbl <- renderDT({
    datatable(
      cohort_data()$wide,
      options = list(scrollX = TRUE, pageLength = 12),
      rownames = FALSE
    )
  })
  
  output$cohort_sankey_plot_ui <- renderUI({
    show_block_loader("cohort_sankey_block", "Будуємо маршрут клієнтів...")
    on.exit(hide_block_loader("cohort_sankey_block"), add = TRUE)
    
    sk <- cohort_sankey_data()
    
    if (is.null(sk) || nrow(sk$nodes) == 0 || nrow(sk$links) == 0) {
      return(
        div(
          class = "info-card",
          style = "min-height: 220px; display:flex; align-items:center;",
          div(
            style = "width:100%;",
            h4("Маршрути для обраного стартового вузла не знайдено"),
            p("За поточними параметрами немає достатньо даних для побудови когортного Sankey."),
            tags$ul(
              tags$li("оберіть інший стартовий вузол"),
              tags$li("зменште мінімальну кількість клієнтів у зв'язку"),
              tags$li("збільшіть глибину маршруту"),
              tags$li("перевірте, чи є наступні покупки після стартового вузла")
            )
          )
        )
      )
    }
    
    dynamic_height <- calc_sankey_height(sk$nodes)
    
    sankeyNetworkOutput(
      "cohort_sankey_plot",
      height = paste0(dynamic_height, "px")
    )
  })
  
  output$cohort_sankey_mode_text <- renderUI({
    sk <- cohort_sankey_data()
    
    msg <- if (!is.null(sk$display_message)) sk$display_message else ""
    
    if (msg == "") return(NULL)
    
    div(
      style = paste(
        "padding:10px 12px;",
        "background:#F8FAFC;",
        "border:1px solid #E2E8F0;",
        "border-radius:8px;",
        "color:#334155;"
      ),
      HTML(msg)
    )
  })
  
  output$cohort_sankey_other_details_tbl <- renderUI({
    sk <- cohort_sankey_data()
    
    if (is.null(sk) || is.null(sk$other_details) || nrow(sk$other_details) == 0) {
      return(
        div(
          class = "info-card",
          style = "min-height: 120px; display:flex; align-items:center;",
          div(
            style = "width:100%;",
            h4("Інших переходів немає"),
            p("Для цього маршруту всі переходи потрапили в основні гілки, додаткових переходів не виявлено.")
          )
        )
      )
    }
    
    tbl <- sk$other_details
    
    if ("level_from" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(
          level_from = case_when(
            level_from == "level_0" ~ "Стартовий крок",
            level_from == "level_1" ~ "2-й крок",
            level_from == "level_2" ~ "3-й крок",
            level_from == "level_3" ~ "4-й крок",
            level_from == "level_4" ~ "5-й крок",
            level_from == "level_5" ~ "6-й крок",
            TRUE ~ level_from
          )
        )
    }
    
    if ("level_to" %in% names(tbl)) {
      tbl <- tbl %>%
        mutate(
          level_to = case_when(
            level_to == "level_0" ~ "Стартовий крок",
            level_to == "level_1" ~ "2-й крок",
            level_to == "level_2" ~ "3-й крок",
            level_to == "level_3" ~ "4-й крок",
            level_to == "level_4" ~ "5-й крок",
            level_to == "level_5" ~ "6-й крок",
            TRUE ~ level_to
          )
        )
    }
    
    # якщо рівень маршруту всюди один і той самий — не показуємо його,
    # щоб не плутати користувача
    show_route_level <- FALSE
    
    if (all(c("step_from", "step_to") %in% names(tbl))) {
      show_route_level <- tbl %>%
        distinct(step_from, step_to) %>%
        nrow() > 1
    }
    
    # робимо одну зрозумілу колонку маршруту лише якщо рівнів декілька
    if (show_route_level && all(c("step_from", "step_to") %in% names(tbl))) {
      tbl <- tbl %>%
        mutate(
          route_level = paste0(step_from, " → ", step_to)
        )
    }
    
    rename_map <- c(
      from_step = "Стартовий вузол",
      to_step = "Наступний вузол",
      from = "Стартовий вузол",
      to = "Наступний вузол",
      to_real = "Наступний ",
      grouped_into = "Згруповано в",
      clients = "Клієнтів",
      transitions_n = "Кількість переходів",
      avg_lag_days = "Сер. лаг, днів",
      median_lag_days = "Медіанний лаг, днів",
      from_clients = "Клієнтів у стартовому вузлі",
      conversion_from_clients = "Конверсія, %",
      level_from = "Рівень старту",
      level_to = "Рівень переходу",
      source = "Стартовий вузол",
      target = "Наступний вузол",
      value = "Клієнтів",
      pct_start = "% від стартової когорти",
      route_level = "Крок маршруту"
    )
    
    existing_map <- rename_map[names(rename_map) %in% names(tbl)]
    if (length(existing_map) > 0) {
      tbl <- tbl %>% rename(!!!setNames(names(existing_map), existing_map))
    }
    
    # якщо рівень не потрібен — прибираємо технічні колонки
    drop_cols <- c()
    
    if (!show_route_level) {
      drop_cols <- c(drop_cols, "step_from", "step_to")
    }
    
    drop_cols <- intersect(drop_cols, names(tbl))
    
    if (length(drop_cols) > 0) {
      tbl <- tbl %>% select(-all_of(drop_cols))
    }
    
    DT::datatable(
      tbl,
      options = list(scrollX = TRUE, pageLength = 10),
      rownames = FALSE
    )
  })
  

  
  output$audit_metrics_tbl <- renderDT({
    show_block_loader("audit_metrics_tbl_block", "Готуємо метрики аудиту...")
    on.exit(hide_block_loader("audit_metrics_tbl_block"), add = TRUE)
    
    req(audit_data())
    
    datatable(
      audit_data()$metrics,
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$audit_missing_tbl <- renderDT({
    show_block_loader("audit_missing_tbl_block", "Готуємо метрики аудиту...")
    on.exit(hide_block_loader("audit_missing_tbl_block"), add = TRUE)
    
    req(audit_data())
    
    datatable(
      audit_data()$missing_tbl,
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$audit_client_activity_summary_tbl <- renderDT({
    
    show_block_loader(
      "audit_client_activity_summary_tbl_block",
      "Аналізуємо активність клієнтів..."
    )
    on.exit(hide_block_loader("audit_client_activity_summary_tbl_block"), add = TRUE)
    
    req(audit_data())
    
    df <- audit_data()$client_activity_summary
    
    total_clients <- sum(df$Значення, na.rm = TRUE)
    
    df <- df %>%
      mutate(
        `% клієнтів` = round(100 * Значення / total_clients, 1),
        `% клієнтів` = paste0(`% клієнтів`, "%")
      )
    
    datatable(
      df,
      options = list(dom = "t", scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$audit_category_top_tbl <- renderDT({
    datatable(
      audit_data()$category_top %>% slice_head(n = 20),
      options = list(pageLength = 10, scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$audit_brand_top_tbl <- renderDT({
    datatable(
      audit_data()$brand_top %>% slice_head(n = 20),
      options = list(pageLength = 10, scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$audit_sum_parse_examples <- renderUI({
    req(audit_data())
    
    df <- audit_data()$sum_parse_examples
    
    if (is.null(df) || nrow(df) == 0) {
      
      div(
        style = "
        padding: 18px;
        background: #F0FDF4;
        border-radius: 12px;
        border: 1px solid #BBF7D0;
        color: #166534;
        font-size: 14px;
      ",
        "✔ Проблемних значень у колонці суми не виявлено"
      )
      
    } else {
      
      DT::datatable(
        df,
        options = list(pageLength = 10, scrollX = TRUE),
        rownames = FALSE
      )
      
    }
  })
  
  output$journey_nodes_tbl <- renderDT({
    sk <- journey_sankey_data()
    
    validate(
      need(!is.null(sk), "Немає даних для відображення."),
      need(nrow(sk$nodes) > 0, "Для обраного вузла немає маршрутів, тому список вузлів недоступний.")
    )
    
    datatable(
      sk$nodes %>%
        select(level, name, full_name) %>%
        mutate(
          Рівень = case_when(
            level == "level_0" ~ "1-й крок (старт)",
            level == "level_1" ~ "2-й крок",
            level == "level_2" ~ "3-й крок",
            level == "level_3" ~ "4-й крок",
            level == "level_4" ~ "5-й крок",
            level == "level_5" ~ "6-й крок",
            TRUE ~ level
          )
        ) %>%
        select(
          Рівень,
          `Коротка назва` = name,
          `Повна назва` = full_name
        ),
      options = list(
        scrollX = TRUE,
        pageLength = 10,
        autoWidth = FALSE,
        columnDefs = list(
          list(width = '150px', targets = 0),
          list(width = '200px', targets = 1),
          list(width = '300px', targets = 2)
        )
      ),
      rownames = FALSE,
      class = "cell-border stripe"
    )
  })
  
  
  
  
  observeEvent(input$file, {
    req(input$file)
    
    file_path <- input$file$datapath
    show_global_loader("Завантажуємо файл і готуємо результати...")
    
    tryCatch({
      df_raw <- read_input_data(file_path)
      
      if (nrow(df_raw) == 0) {
        stop("Файл порожній")
      }
      
      df_clean <- prepare_cleaned_data(df_raw)
      
      loaded_data(df_raw)
      cleaned_data_val(df_clean)
      
      rebuild_analysis()
      
      session$onFlushed(function() {
        hide_global_loader()
        showNotification("Файл успішно завантажено. Результати оновлено.", type = "message", duration = 4)
      }, once = TRUE)
      
    }, error = function(e) {
      loaded_data(NULL)
      cleaned_data_val(NULL)
      events_data_val(NULL)
      event_links_val(NULL)
      transitions_summary_val(NULL)
      audit_data_val(NULL)
      
      hide_global_loader()
      showNotification(
        paste("Помилка завантаження:", conditionMessage(e)),
        type = "error",
        duration = 8
      )
    })
  })
  
  observeEvent(
    list(
      input$node_mode,
      input$basket_mode,
      input$min_days,
      input$use_max_days,
      input$max_days
    ),
    {
      req(cleaned_data())
      
      show_global_loader("Оновлюємо розрахунки...")
      
      tryCatch({
        rebuild_analysis()
        session$onFlushed(function() {
          hide_global_loader()
        }, once = TRUE)
      }, error = function(e) {
        hide_global_loader()
        showNotification(
          paste("Помилка оновлення:", conditionMessage(e)),
          type = "error",
          duration = 8
        )
      })
    },
    ignoreInit = TRUE
  )
  
  observeEvent(
    list(
      input$node_mode,
      input$basket_mode,
      input$min_days,
      input$max_days,
      input$journey_depth,
      input$journey_top_n,
      input$journey_min_clients,
      input$journey_label_chars,
      input$cohort_depth,
      input$cohort_top_n,
      input$cohort_min_clients,
      input$cohort_label_chars,
      input$sankey_start_node,
      input$cohort_sankey_start_node
    ),
    {
      req(cleaned_data())
      
      show_global_loader("Оновлюємо розрахунки...")
      
      session$onFlushed(function() {
        hide_global_loader()
      }, once = TRUE)
    },
    ignoreInit = TRUE
  )
  
  
  
  
}

shinyApp(ui, server)