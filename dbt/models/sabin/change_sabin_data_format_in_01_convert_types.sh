#!/bin/bash

# Nome do arquivo
arquivo="sabin_01_convert_types.sql"

# Verifica se a substituição já foi feita
if grep -q 'DD/MM/YYYY' "$arquivo"; then
    # Faz a substituição no arquivo usando sed
    sed -i 's/DD\/MM\/YYYY/MM\/DD\/YYYY/g' "$arquivo"
    echo "Substituição 'DD/MM/YYYY' por 'MM/DD/YYYY' concluída!"
elif grep -q 'MM/DD/YYYY' "$arquivo"; then
    # Faz a substituição inversa no arquivo usando sed
    sed -i 's/MM\/DD\/YYYY/DD\/MM\/YYYY/g' "$arquivo"
    echo "Substituição 'MM/DD/YYYY' por 'DD/MM/YYYY' concluída!"
else
    echo "Nenhuma substituição necessária. O arquivo já está atualizado."
fi
