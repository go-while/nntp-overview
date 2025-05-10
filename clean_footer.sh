#!/bin/bash

exit 1

# clean footer
while IFS="" read file; do
test -e "$file.bak" && echo "error: exists $file.bak" && continue
cp -v "$file" "$file.bak"
sed -E '/^(\x0|EOV|time=|EOF)/d' -i "$file"
chown rto:rto "$file"
done< <(find . -maxdepth 1 -type f -name "*.overview"|sort -h)


# restore bak files
while IFS="" read bakfile; do
oldfile=$(echo "$bakfile"|sed 's/\.bak$//g')
mv -v "$bakfile" "$oldfile"
chown rto:rto "$oldfile"
done< <(find . -maxdepth 1 -type f -name "*.overview.bak"|sort -h)


