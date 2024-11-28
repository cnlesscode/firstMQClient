time=$(date "+%Y-%m-%d %H:%M:%S")
desc="push on ${time}"
git add .
git commit -m "${desc}"
git push -u origin master