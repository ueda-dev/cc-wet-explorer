# CC-WET-EXPLORER
## TL;DR
Common-Crawlが提供するWETファイルに対して、キーワード検索を行うプログラムです。

## How to use
1. このリポジトリをクローンする
```
git clone https://github.com/ueda-dev/cc-wet-explorer.git
```

2. 必要なライブラリをインストールする（必要に応じて仮想環境を作成してください）
```
cd cc-wet-explorer
python -m venv .venv (optional)
pip install -r requirements.txt
```

3. src/dependencies/keywords.txtを編集し、検索したい単語を記述する。
```
(例)
Amazon
amazon
アマゾン
ｱﾏｿﾞﾝ
あまぞん
```

4. main.pyを実行する。
```
python main.py
```