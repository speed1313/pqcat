# pqcat
[![pypi](https://img.shields.io/pypi/v/pqcat.svg)](https://pypi.python.org/pypi/pqcat) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

pqcat is a fast command-line tool for inspecting Parquet files.

## Installation

```bash
pip install pqcat
```


## Usage

```bash
$ pqcat cat examples/retail.parquet | head
shape: (50, 8)
┌─────────┬────────────┬────────────┬───────────┬───────────┬──────────┬────────┬─────────────┐
│ OrderID ┆ CustomerID ┆ OrderDate  ┆ ProductID ┆ Category  ┆ Quantity ┆ Price  ┆ TotalAmount │
│ ---     ┆ ---        ┆ ---        ┆ ---       ┆ ---       ┆ ---      ┆ ---    ┆ ---         │
│ str     ┆ str        ┆ str        ┆ str       ┆ str       ┆ i64      ┆ f64    ┆ f64         │
╞═════════╪════════════╪════════════╪═══════════╪═══════════╪══════════╪════════╪═════════════╡
│ O0001   ┆ C1007      ┆ 2024-01-27 ┆ P016      ┆ Clothing  ┆ 1        ┆ 159.57 ┆ 159.57      │
│ O0002   ┆ C1001      ┆ 2024-01-01 ┆ P011      ┆ Toys      ┆ 3        ┆ 10.54  ┆ 31.62       │
│ O0003   ┆ C1011      ┆ 2024-01-23 ┆ P011      ┆ Clothing  ┆ 1        ┆ 82.04  ┆ 82.04       │
│ O0004   ┆ C1020      ┆ 2024-01-18 ┆ P006      ┆ Toys      ┆ 5        ┆ 174.84 ┆ 874.2       │
```

