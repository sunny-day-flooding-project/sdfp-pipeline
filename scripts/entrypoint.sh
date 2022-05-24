#!/bin/bash

# Pressure data processing
python process_pressure.py

# Drift-correction
python drift_correction.py