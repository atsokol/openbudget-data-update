library(readr)
library(dplyr)
library(tidyr)
library(lubridate)

# Load categories
categories <- read_csv("inputs/budget_categories.csv")

inc_categ <- categories |>
  filter(CATEG=="INC")

exp_categ <- categories |>
  filter(CATEG=="EXP")

incomes <- read_csv("data/incomes.csv")
expenses <- read_csv("data/expenses.csv")

inc_agg <- incomes |>
  mutate(TYPE = cut(COD_INCO,
                    breaks = c(0,inc_categ$BREAK_END),
                    labels = c(inc_categ$NAME_TYPE))
  ) |>
  filter(FUND_TYP == "T", TYPE %in% c("Tax", "Non-tax", "Transfers")) |>
  group_by(CITY, REP_PERIOD) |>
  summarise(income = sum(FAKT_AMT) / 10^6)

core_inc_agg <- incomes |>
  mutate(TYPE = cut(COD_INCO,
                    breaks = c(0,inc_categ$BREAK_END),
                    labels = c(inc_categ$NAME_TYPE))
  ) |>
  filter(FUND_TYP == "T", TYPE %in% c("Tax", "Non-tax")) |>
  group_by(CITY, REP_PERIOD) |>
  summarise(core_income = sum(FAKT_AMT) / 10^6)

exp_agg <- expenses |>
  mutate(TYPE = cut(COD_CONS_EK,
                    breaks = c(0,exp_categ$BREAK_END),
                    labels = c(exp_categ$NAME_TYPE)
  )) |>
  filter(FUND_TYP == "T", TYPE %in% c("Opex")) |>
  group_by(CITY, REP_PERIOD) |>
  summarise(expense = sum(FAKT_AMT) / 10^6)

op_surplus <- left_join(inc_agg,
                        exp_agg,
                        by = join_by(CITY == CITY, REP_PERIOD == REP_PERIOD)) |>
  left_join(core_inc_agg,
            by = join_by(CITY == CITY, REP_PERIOD == REP_PERIOD))
  mutate(op_surplus = income - expense)

write_csv(op_surplus, "data/data_analysis.csv")

transfer_share <- incomes |>
  mutate(TYPE = cut(COD_INCO,
                    breaks = c(0,inc_categ$BREAK_END),
                    labels = c(inc_categ$NAME_TYPE))
  ) |>
  filter(FUND_TYP == "T", TYPE %in% c("Tax", "Non-tax", "Transfers")) |>
  group_by(CITY, REP_PERIOD, TYPE) |>
  summarise(income = sum(FAKT_AMT) / 10^6) |>
  mutate(YEAR = year(REP_PERIOD)) |>
  group_by(CITY, YEAR, TYPE) |>
  summarise(income = sum(income)) |>
  pivot_wider(names_from = TYPE, values_from = income) |>
  mutate(transfer_pct = Transfers / (Tax + `Non-tax` + Transfers)) |>
  filter(YEAR %in% c(2021,2024)) |>
  select(CITY,YEAR,transfer_pct) |>
  pivot_wider(names_from = YEAR, values_from = transfer_pct) |>
  rename(Y2024 = `2024`, Y2021 = `2021`) |>
  arrange(desc(Y2024))

write_csv(transfer_share, "data/transfer_share.csv")
