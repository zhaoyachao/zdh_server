classp=
for jar in `ls $1/libs`;
do
if [ -z "$classp" ]; then
   classp="$1/libs/$jar"
else
classp="$classp,$1/libs/$jar"
fi
done
echo $classp
