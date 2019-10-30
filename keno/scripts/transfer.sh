shopt -s nullglob
arr=(*)

for i in "${arr[@]}"
do
    
    t_dir=($i/exercises/src/*)
    if [ ${#t_dir[@]} -gt 0 ]
    then
        # mkdir "t_"$i
        # cp -R $i/exercises/src/ /src/$i/
        
        for j in "${t_dir[@]}"
        do
            echo $j
        done
    fi
done