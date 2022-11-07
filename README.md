## Instalação

Para instalar este pipeline, instale primeiro `conda` e na sequência `mamba`:

- Instalação do Conda: [clique aqui](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)

- Instalação do Mamba, execute este comando, uma vez que Conda esteja instalado:
``` bash
conda install mamba -n base -c conda-forge
```
alternativa:
``` bash
conda install -n base conda-forge::mamba
```
P.S.: arquivo psutil pode corromper a instalação, caso isso ocorrer, deletar a pasta com psutil e reiniciar instalação do mamba


Agora clone este repositório:
Clone this repository `ncov`
```bash
git clone https://github.com/andersonbrito/itps_respat.git
```

Uma vez instalados `conda` e `mamba`, acesse o diretório `config`, e execute os seguintes comandos para criar o ambiente conda `diag`:

```bash
 mamba create -n diag
 mamba env update -n diag --file environment.yaml
 ```

Por fim, ative o ambiente `diag`:
```bash
conda activate diag
```

## Execução

Para executar o pipeline até o último passo, execute o seguinte comando, com o ambiente `diag` ativado:
```bash
snakemake all --cores all
```
### Linux
Este pipeline esta otimizado para ambiente MAC OS. Para executar no ambiente Linux necessário alterar as chamadas com `sed`, por exemplo no arquivo Snakefile:

```bash
# lin 222
sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}

# line 398
sed -i 's/{params.test_col}/test_result/' {output}
```

Ref: [macOS - sed command with -i option failing on Mac, but works on Linux - Stack Overflow](https://stackoverflow.com/questions/4247068/sed-command-with-i-option-failing-on-mac-but-works-on-linux)