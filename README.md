make-ipinyou-data
=================

This repository converts the iPinYou RTB dataset into a clean, research-ready format.

### Step 0: Download the source data
Download `ipinyou.contest.dataset.zip` from [Kaggle](https://www.kaggle.com/datasets/lastsummer/ipinyou) and unzip it. You should now have a folder named `ipinyou.contest.dataset`.

### Step 1: Point `original-data` to the dataset
Create or refresh the symlink inside `original-data` so it references your local copy:
```
weinan@ZHANG:~/Project/make-ipinyou-data/original-data$ ln -sfn ~/Data/ipinyou.contest.dataset ipinyou.contest.dataset
```
After linking, `make-ipinyou-data/original-data/ipinyou.contest.dataset` should contain the raw files:
```
weinan@ZHANG:~/Project/make-ipinyou-data/original-data/ipinyou.contest.dataset$ ls
algo.submission.demo.tar.bz2  README         testing2nd   training3rd
city.cn.txt                   region.cn.txt  testing3rd   user.profile.tags.cn.txt
city.en.txt                   region.en.txt  training1st  user.profile.tags.en.txt
files.md5                     testing1st     training2nd
```
Do not unzip the archives in the campaign subfolders; the build scripts handle them.

### Step 2: Build the processed data
From the repository root, run `make all`. Once the pipeline finishes (expect roughly 14 GB of output), the folder structure will look similar to:
```
weinan@ZHANG:~/Project/make-ipinyou-data$ ls
1458  2261  2997  3386  3476  LICENSE   mkyzxdata.sh   python     schema.txt
2259  2821  3358  3427  all   Makefile  original-data  README.md
```
Each numbered directory corresponds to a campaign (for example `1458`). The `all` directory aggregates every campaign and can be removed if you do not need the combined dataset.

### Using the processed data
Campaign `1458` is representative:
```
weinan@ZHANG:~/Project/make-ipinyou-data/1458$ ls
featindex.txt  test.log.txt  test.yzx.txt  train.log.txt  train.yzx.txt
```
* `train.log.txt` and `test.log.txt` store the original string features per impression. Column 1 is the click label, column 14 is the clearing price.
* `featindex.txt` maps categorical feature values to integer ids, e.g. `8:115.45.195.*	29` means column 8 with value `115.45.195.*` becomes feature id `29`.
* `train.yzx.txt` and `test.yzx.txt` hold the vectorised format described in [iPinYou Benchmarking](http://arxiv.org/abs/1407.7073): `y` is the click label, `z` is the winning price, and `x` contains `feature_id:1` pairs.

Questions? Please open an issue or contact [Weinan Zhang](http://www0.cs.ucl.ac.uk/staff/w.zhang/).
