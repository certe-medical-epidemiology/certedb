# The `certetemplate` package for R

### General Information

<img src="https://www.certe.nl/img/logo.png" height="144px" />

This is an R package developed by [**Certe**](https://www.certe.nl), a non-profit medical laboratory in the Northern Netherlands that provides routine diagnostic tests for clinical chemistry and clinical microbiology, as well as medical logistics and a thrombosis service. Their department of Medical Epidemiology, which developed this R package, conducts (and develops new methods for) medical data analyses, for both routine workflows and scientific research.

For all our packages, please visit [our GitHub organisation overview](https://github.com/certe-medical-epidemiology). Our R packages are not on CRAN since their use is primarily intended for own staff, but they are publicly available to support open science. All our R packages are published [here at R-universe](https://certe-medical-epidemiology.r-universe.dev), allowing anyone to install and update the packages using common methods, such as the RStudio menu bar or `install.packages()`. To use the R-universe of Certe Medical Epidemiology, run:

```r
# set the repos option to include the Certe R-universe
options(repos = c(
  CerteMedEpi = "https://certe-medical-epidemiology.r-universe.dev",
  CRAN = "https://cloud.r-project.org"))

# our 'loader package' certedata installs all Certe R packages:
install.packages("certedata")
```

### Copyright regarding this software

We believe open science matters, which is why this package is available on GitHub.

This R package is free, open-source software and licensed under the [GNU General Public License v2.0 (GPL-2)](./LICENSE.md). In a nutshell, this means that this package:

- May be used for commercial purposes
- May be used for private purposes
- May **not** be used for patent purposes
- May be modified, although:
  - Modifications **must** be released under the same license when distributing the package
  - Changes made to the code **must** be documented
- May be distributed, although:
  - Source code **must** be made available when the package is distributed
  - A copy of the license and copyright notice **must** be included with the package.
- Comes with a LIMITATION of liability
- Comes with NO warranty

© Certe Medical Diagnostics & Advice Foundation. Certe is the sole copyright holder and funder of this software.
