# yad2grabber
A script to download all apartments from [yad2](https://yad2.co.il/realestate/rent) I am iterested in   

## Usage
please don't

### Grabber
First, use `spider.py` to download all ads:
```bash
python3 spider.py
```

### Web view
Then you can watch downloaded ads in a web-browser:
```bash
uvicorn main:app --reload --port 8000
```

Open `localhost:8000` and you'll see the list (although not beautiful)

## Goal
- download all ads near any train station into a single base
- make UI to select ads and mark them as processed/unprocessed, add own notes