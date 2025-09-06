# buzzline-03-schroder  

**Course:** Streaming Data – Module 3  
**Date:** September 5, 2025  
**Author:** Justin Schroder  
**GitHub:** [SchroderJ-pixel](https://github.com/SchroderJ-pixel) 

---

## Overview

For this project I built two small, real-time pipelines on Kafka:

- **Nutrition (JSON):** I stream meals/macros from `data/food.json` → Kafka → a consumer does lightweight live checks (protein goal, late eating, carb spikes, under-fueling).
- **Class Attendance (CSV→JSON):** I stream `data/attendance.csv` → Kafka → a consumer flags rolling/daily attendance drops, late bursts, and chronic absences.

I **edit files in VS Code**, run **Kafka in WSL (Ubuntu)**, and run **Python in a PowerShell terminal** with a virtual environment. Each pipeline has its own Kafka topic (see `.env`).

---

## Where to run what (cheat sheet)

- **VS Code editor**: open/edit files (`.env`, `*.py`, data files).
- **VS Code PowerShell terminal (Windows)**: Python/venv, *run producers & consumers*, Git commands.
- **VS Code WSL (Ubuntu) terminal**: *Kafka server* and Kafka admin commands (`kafka-topics.sh`, etc).

> When in doubt:
> - Kafka things → **WSL**  
> - Python things → **PowerShell**  
> - File edits → **VS Code editor**

---

## Prereqs (Modules 1 & 2)

Complete:  
- <https://github.com/denisecase/buzzline-01-case>  
- <https://github.com/denisecase/buzzline-02-case>  
**Python 3.11 is required.**

I forked/renamed the template to `buzzline-03-schroder` and work locally.

---

## Task 0 — If Windows, launch WSL (from PowerShell)

**Where:** VS Code **PowerShell terminal**

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window. 

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground.
5. Keep this terminal open - Kafka will run here
6. Watch for "started (kafka.server.KafkaServer)" message

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment
2. Activate the virtual environment
3. Upgrade pip
4. Install from requirements.txt

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Start a Kafka JSON Producer

This producer generates streaming JSON data for our topic.

In VS Code, open a terminal.
Use the commands below to activate .venv (if not active), and start the producer.

Windows:

```shell
.venv\Scripts\activate
py -m producers.json_producer_schroder
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m producers.json_producer_schroder
```

What did we name the topic used with JSON data?
Hint: See the producer code and [.env](.env).

## Task 4. Start a Kafka JSON Consumer

This consumer processes streaming JSON data.

In VS Code, open a NEW terminal in your root project folder.
Use the commands below to activate .venv, and start the consumer.

Windows:

```shell
.venv\Scripts\activate
py -m consumers.json_consumer_schroder
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m consumers.json_consumer_schroder
```

What did we name the topic used with JSON data?
Hint: See the consumer code and [.env](.env).

---

## Task 5. Start a Kafka CSV Producer

Follow a similar process to start the csv producer.
You will need to:

1. Open a new terminal (yes another)!
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run. Hint: Look in the producers folder.

What did we name the topic used with csv data?
Hint: See the producer code and [.env](.env).

Hint: Windows:

```shell
.venv\Scripts\activate
py -m producers.csv_producer_schroder
```

## Task 6. Start a Kafka CSV Consumer

Follow a similar process to start the csv consumer.
You will need to:

1. Open a new terminal (yes another)!
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run. Hint: Look in the consumers folder.

What did we name the topic used with csv data?
Hint: See the consumer code and [.env](.env).

Hint: Windows:

```shell
.venv\Scripts\activate
py -m consumers.csv_consumer_schroder
```

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

# What the files do

My **Nutrition (JSON)** pipeline streams meal events from `food.json` into Kafka and a consumer tallies daily macros in real time. It also fires quick alerts for big carb spikes, under-fueling after workouts, late high-calorie meals, and hitting the daily protein goal.

My **Attendance (CSV→JSON)** pipeline streams rows from `attendance.csv` into Kafka and the consumer watches per-course attendance. It flags rolling/daily drops, bursts of “late” arrivals, and students with repeated absences — with simple thresholds I can tweak in `.env`.


## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.
