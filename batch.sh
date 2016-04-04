echo "Script 1a";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=.5 p_value_threshold=.05 >| output/batch_output1a.txt
echo "Script 1b";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=.5 p_value_threshold=.01 >| output/batch_output1b.txt
echo "Script 1c";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=.5 p_value_threshold=.001 >| output/batch_output1c.txt

echo "Script 2a";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False LIMIT_INFREQUENT_EVENTS=50 LIMIT_INFREQUENT_PAIRS=50 p_value_threshold=.05 >| output/batch_output2a.txt
echo "Script 2b";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False LIMIT_INFREQUENT_EVENTS=50 LIMIT_INFREQUENT_PAIRS=50 p_value_threshold=.01 >| output/batch_output2b.txt
echo "Script 2c";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False LIMIT_INFREQUENT_EVENTS=50 LIMIT_INFREQUENT_PAIRS=50 p_value_threshold=.001 >| output/batch_output2c.txt


echo "Script 3a";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False p_value_threshold=.05 >| output/batch_output3a.txt
echo "Script 3b";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False p_value_threshold=.01 >| output/batch_output3b.txt
echo "Script 3c";
python amfamTpattern.py WINDOW=24 KL_CUTOFF=False p_value_threshold=.001 >| output/batch_output3c.txt



echo "Script 4a";
python amfamTpattern.py WINDOW=False KL_CUTOFF=False p_value_threshold=.05 >| output/batch_output4a.txt
echo "Script 4b";
python amfamTpattern.py WINDOW=False KL_CUTOFF=False p_value_threshold=.01 >| output/batch_output4b.txt
echo "Script 4c";
python amfamTpattern.py WINDOW=False KL_CUTOFF=False p_value_threshold=.001 >| output/batch_output4c.txt