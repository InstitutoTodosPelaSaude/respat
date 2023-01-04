# Pipeline para múltiplos laboratórios
Pipeline para análise de múltiplos patógenos - COVID-19 taxa de positividade

## Instalação

Para instalar este pipeline, instale primeiro `conda` e na sequência `mamba`:

- Instalação do Conda: [clique aqui](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)

- Instalação do Mamba, execute este comando, uma vez que Conda esteja instalado:
``` bash
conda install mamba -n base -c conda-forge
```
Alternativamente:
``` bash
conda install -n base conda-forge::mamba
```
P.S.: arquivo psutil pode corromper a instalação. Se isso ocorrer, delete a pasta com psutil e reinicie instalação do mamba.


Agora clone este repositório:
Clone this repository `respat`
```bash
git clone https://github.com/InstitutoTodosPelaSaude/respat.git
```

Uma vez instalados `conda` e `mamba`, acesse o diretório `config`, e execute os seguintes comandos para criar o ambiente conda `diag`:

```bash
 mamba create -n diag
 mamba env update -n diag --file mon.yaml
 ```

Por fim, ative o ambiente `diag`:
```bash
conda activate diag
```

## Execução

Para executar o pipeline até o último passo, execute o seguinte comando, com o ambiente `mon` ativado:
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

## Atenção com atualizações de pacotes no env

Pacote tabulate não deve migrar para versão 0.9 - corrompe processamento do snakefile.

Correção pelo terminal, dentro da env, fazer downgrade para versão tabulate 0.8.1
```bash
mamba install -c conda-forge tabulate=0.8.9
```
