# OpenOBA
## About OBA and the Framework
<aside>
📌 The **OpenOBA** Framework (*Framework for Online Behavioral Advertising Measurement*) is a Python web privacy experimentation tool that measures and analyses the occurrence of Online Behavioral Advertising resulting from specific browsing behavior set up by the user or researcher.

</aside>

Based on [Bannerclick's](https://github.com/bannerclick/bannerclick) version of the [OpenWPM](https://github.com/openwpm/OpenWPM) framework, OpenOBA provides a flexible and easy-to-set-up environment where highly configurable experiments involving web crawlers and ad capture can be created and run.

After each experiment's successful run, its configuration parameters, browser profile, and browsing data are saved. This allows the user to load any created experiment to keep feeding the browser with the specified behavior, or to analyze the data collected until that point to measure its OBA occurrence.

**What does it mean to measure OBA?**

<aside>
👉 Measuring OBA means quantifying a user's exposure to online advertisements targeted specifically to him, based on his past web browsing behavior as a result of *web tracking*.

</aside>

For a user to be shown targeted ads, his activity and interests must have been profiled and narrowed down to specific *categories* on the browsers he has used. 

To quantify this phenomenon, we require all of the ads that were shown to the user together with the information about their *content/category* so that we can get how many of them were related to the user’s profile *category*.

## Installation

### 0. Prerequisites

OpenOBA is built on top of Bannerclick's OpenWPM framework ver `0.21.0`. It uses the following versions of its parts as reference:

First prerequisite is mamba, which will be used to install the *openwpm* conda environment. As stated in the [mamba installation guide](https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html), we can use *`[miniforge](https://github.com/conda-forge/miniforge)`.* To install it we can simply 

```bash
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-$(uname)-$(uname -m).sh
```

### 1. Cloning the repository

```bash
git clone https://github.com/fukuda-lab/maormeno-famoba.git
cd maormeno-famoba
```

### 2. Install OpenWPM

We will use the same script of OpenWPM, the only change is that we want to use specifically Firefox version `108.0.2`

```bash
./install.sh
```

### 3. Install additional dependencies

If the last step was successful, we can now just install the missing dependencies. To do this, we have to activate the conda `openwpm` environment by running:

```bash
conda activate openwpm
```

In case there is some dependency problem, see if there are any mismatches between `environment.yaml` and `requirements.txt`. If so, try installing `requirements.txt`

```bash
pip install -r requirements.txt
```

### 4. Run demo.py

If everything is working correctly, we should be able to run `[demo.py](http://demo.py)` file (with the openwpm env activated)

```bash
python oba_crawler_demo.py
```

- TO RUN IN MACOS:
    
    `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` before any python command:
    
    ```bash
    OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python oba_crawler_demo.py
    ```


# Documentation

- **Categorizer** *(private class, internal use only)*: Given valid credentials, using WebShrinker API is able to categorize URLs with the IAB taxonomy or WebShrinker own Taxonomy.
- **TrainingPagesHandler** *(public class if a user wants to access its functionalities, intended to be used just by OBACrawler class)*:
This class has several functionalities, but in summary:
    1.  it takes charge into fetching training pages from tranco and saving them in a file
    2.  loading them from already fetched previous dates
    3.  categorizing any given set of training pages with the Categorizer (either loaded from Tranco or a custom training pages list provided by the user) and saving the training pages in a SQLite database categorized
    4.  given already categorized pages in an SQLite database, return a list of all the training pages that belong to an input category
    5. more methods related to cookie banners presence of training pages
- **OBAMeasurementExperiment** *(public class, directly used by the users)*: This is the entrypoint for the framework to run the crawlings and pages.
This class handles the setup of the environment according to the arguments values, it includes the calls to the TrainingPagesHandler. Functions include:
    1. **init**, the setup (initializer) where it can either create a new experiment or load an old one, load either pages from Tranco Top, or from a custom list and can either categorize the lists for them or not, making. the validations accordingy.
    2.  Filter and set the training pages by category in case they were categorized beforehand.
    3.  Run the actual crawling for the experiment, saving the ad urls found in the control_sites for advertisements, and adding all the necessary data about the visits to the sites for them to be analyzed later. It also handles the saving of the browser profiles to be then loaded when wanting to resume a previously started experiment.
- **DataProcesser** *(public to the user)*: This is the other entrypoint for the Framework. It should only recieve the experiment_name. With that name it can connect to the sqlite database with all the crawling data (site visits, browser ids, etc), to the {experiment_name}_config.json. It is in charge of resolving the Ad URLs after being extracted during an experiment run
- **ExperimentMetrics** (public): Used to get several insights about the experiment after having its ads processed
- **OBAAnalysis** (public): ****Is also in charge of getting insights about the experiment but are more directly related to OBA.
- Several other scripts in third_party_analysis and oba_analysis that are used to generate tables and analysis for the resources of an experiment (these are more up-to-date in the `control-pages-control-run` branch).



## Experiment run
*This is a full example of an experiment run using **OpenOBAM***
### Description
<aside>
👉 We want to measure the impact of users’ choice of cookie banners on the exposure to OBA they will receive.

</aside>

For this, we would need to run three different experiment instances, with the same parameters, but with a different `cookie_banner_action`  each. 

In this tutorial, we will show how to run one of those experiment instances to show some of the **OpenOBA** features.

### I. Pre-Crawling phase

1. Create a new experiment
    
    First, create a dictionary with the corresponding arguments
    
    `experiment_name` and `fresh_experiment` parameters are required, the rest depend on the experiment. In this case:
    
    - `cookie_banner_action` of 1: accept all cookies when asked while training
    - `tranco_pages_params`: training pages will be retrieved from an updated list of Tranco most popular sites, of a `size` of 100000
    - We need valid `webshrinker_credentials` because we will need to categorize the pages. This must be provided by the user.
    
    ```python
    oba_cookie_banner_experiment_with_categorization = {
            "experiment_name": "example_clothing_accept_cookie_banner_experiment",
            "fresh_experiment": True,
            "cookie_banner_action": 1,
            "tranco_pages_params": {
                "updated": True,
                "size": 100000,
                },
    				# Real values should be provided by the user
            "webshrinker_credentials": {"api_key": API_KEY, "secret_key": SECRET_KEY},
        }
    ```
    

Create the experiment (this will take some time because the pages need to be categorized)

```python
from oba_crawler import OBAMeasurementExperiment

experiment = OBAMeasurementExperiment(**oba_cookie_banner_experiment_with_categorization)
```

1. Set the training pages for the experiment
    
    <aside>
    👉 **Loading an experiment**
    
    If we first just created the experiment, and now in another script or run we want to load it, we can just do: 
    
    ```python
    experiment = OBAMeasurementExperiment(experiment_name="example_clothing_accept_cookie_banner_experiment", fresh_experiment=False)
    ```
    
    </aside>
    
    Now, since we are using the tranco pages, to set our training pages we need a category, we will pick `Clothing` since we know it has cookie banners (we have to do this every time we want to run an experiment):
    
    ```python
    experiment.set_training_pages_by_category(category="Clothing")
    ```
    
    and we can start the crawling
    

### II. Crawling (training)

With an experiment created, loaded into an instance of `OBAMeasurementExperiment` and  categories set (in case of using tranco), we can start an instance of the crawling for the amount of time that we desire.

```python
experiment.start(hours=3, minutes=30, browser_mode="headless")
```

This will always first run clean visits over the control pages (in clear browsers), so we gather ads that we know that are not due to OBA, and then the *training + control* process will start.

### IV. Data Processing
Now we are ready to use the DataProcesser to get the landing pages for the ads.