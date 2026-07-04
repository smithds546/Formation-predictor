# ⚽ Formation Predictor

A PyTorch pipeline that recommends the optimal defensive **formation type** (3-back, 4-back, or 5-back) for a football team ahead of a match, based on the opponent's setup, recent form, home advantage, and head-to-head history.

Built as coursework for the *Advanced AI* module at Loughborough University.

## Overview

Modern football management involves choosing between 20+ named formations, but the underlying tactical decision is really about the **defensive system**: how many defenders to field. This project frames formation selection as a 3-class classification problem and trains a feedforward neural network in PyTorch to predict it from match context.

Given:
- the opponent's formation type
- whether the match is home or away
- each team's recent winning form
- each team's win rate over their last 10 matches
- head-to-head history between the two teams

...the model outputs a recommended defensive system: **3-back**, **4-back**, or **5-back**.

## Dataset

- **Source:** [SportMonks API](https://www.sportmonks.com)
- **Competition:** Danish Superliga, 2010–2024
- **Size:** ~2,860 fixtures, expanded to ~5,700 team-level records (each match contributes a winner's and loser's perspective)
- **File:** [`danish_superliga_fixtures.csv`](danish_superliga_fixtures.csv)

Each row in the raw dataset is one fixture: teams, goals, formations used by each side, goal difference, and result (H/A/D).

> The Danish Superliga was chosen because SportMonks provides consistent formation data for it across a long historical window, which is not the case for all leagues on the free API tier.

## Pipeline

The full pipeline lives in [`Formation_predictor.ipynb`](Formation_predictor.ipynb) and follows these stages:

1. **Data loading** — read the raw fixtures CSV
2. **Preprocessing** — drop draws (undecided outcomes are noisy for this task), sort chronologically, and expand each match into two team-level records (winner + loser perspective)
3. **Feature engineering** — engineer 10 features per record, including:
   - opponent's formation type (0/1/2)
   - home/away flag
   - each team's most common formation in recent wins
   - each team's win rate over their last 10 matches
   - head-to-head win rate
   - interaction features (win-rate difference, home strength, formation matchup)
4. **Data visualisation** — formation frequency, class balance, and home vs away win rates
5. **Train/val/test split** — 70/15/15, stratified by class
6. **Model architectures** — three PyTorch networks are implemented and compared:

   | Model | Layers | Regularisation | Parameters |
   |---|---|---|---|
   | `SimpleNetwork` | 2 hidden | None | ~2,000 |
   | `MediumNetwork` | 3 hidden | Dropout | ~11,800 |
   | `DeepNetworkBN` | 3 hidden | Batch norm + dropout | ~12,000 |

7. **Training** — Adam optimiser, class-weighted cross-entropy loss (to counter the ~82% class imbalance towards 4-back formations), learning rate scheduling, and early stopping
8. **Evaluation** — confusion matrix, per-class recall/F1, and macro-F1/MCC as headline metrics (accuracy alone is misleading on this imbalanced dataset)
9. **SMOTE comparison** — an alternative to class weighting, oversampling minority classes, compared side-by-side against the class-weighted model
10. **Hyperparameter comparison** — architecture, dropout, and learning rate swept and compared in a single table
11. **Feature importance** — permutation importance to check which features the model actually relies on
12. **Inference** — a `predict_formation()` function that returns a recommended formation type for a given match context

## Results

- The trained model beat the majority-class baseline (~82% accuracy from always predicting 4-back) by roughly **8.8 percentage points on macro-F1**
- `MediumNetwork` with dropout gave the best trade-off between accuracy and training stability
- Home advantage is measurable: home teams win noticeably more often than away teams
- Class weighting handled the imbalance more reliably than SMOTE for this dataset size

Full discussion, confusion matrices, and per-class metrics are in Section 10 onward of the notebook.

## Repository structure

```
Formation-predictor/
├── Formation_predictor.ipynb      # Full pipeline: data → features → training → evaluation
├── danish_superliga_fixtures.csv  # Raw match data (2010–2024)
└── Data/
    └── ultimate_fetcher.py        # Script used to pull fixture data from the SportMonks API
```

## Getting started

### Requirements

```
torch
numpy
pandas
matplotlib
seaborn
scikit-learn
imbalanced-learn
```

Install with:

```bash
pip install torch numpy pandas matplotlib seaborn scikit-learn imbalanced-learn
```

### Running the notebook

1. Clone the repo and make sure `danish_superliga_fixtures.csv` is in the same folder as the notebook.
2. Open `Formation_predictor.ipynb` in Jupyter.
3. Run all cells top to bottom. Model checkpoints are saved automatically during training.
4. To fetch fresh data instead of using the provided CSV, set a `SPORTMONKS_API_TOKEN` environment variable and run `Data/ultimate_fetcher.py`.

## Limitations & future work

- With 20+ raw formations collapsed into 3 defensive types, some tactical nuance is lost — a natural extension would be predicting the full formation string
- The dataset is limited to one league; results may not generalise to leagues with different tactical trends
- SMOTE's synthetic samples didn't clearly outperform class weighting here, likely due to the small size of the minority classes — worth revisiting with a larger dataset

## References

See Section 15 of the notebook for full citations, including the SportMonks API, PyTorch and scikit-learn documentation, and academic sources on class imbalance and regularisation.
