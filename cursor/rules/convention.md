# Regras de estilo e comportamento do projeto

## 1. Formato de escrita e nomenclatura

- Use **snake_case** como padrão principal para:
  - variáveis
  - funções
  - parâmetros
  - propriedades de objetos
-  Use **SCREAMING_SNAKE_CASE** como padrão principal para 
  - constantes 
  - variaveis de ambiente 
- Use **Classe** como padrao para
  - denominar classes

- Nomenclatura de funções e métodos:
  - Preferência forte: nomes **em ingles** (claro, descritivo e natural)
  - Exemplos bons: `validade_cpf`, `create_data`, `is_valid`, `validarCpf`
  - Metodos de classe devem indicar acao.  ao exemplo dos metodos booleados como se fossem uma pergunta exemplos: is_valid(self),is_number(field), is_.... 
  - Quando o nome  ficar muito longo (> ~25 caracteres) ou soar forçado → nao usar cronimos ou reducionismos.. use um termo mais simples
  - Testes devem comentar o que esta sendo testado ou o resultado esperado " should return valueX.. returned valueY"
  - Testes devem ter no nome o nome da funcao testada : test_is_valid, test_create_data...
  - Exemplos aceitáveis: `fetch_user_profile`, `debounce_input`, `generate_pdf`, `parse_token`

- nao use abreviacoes e acronimos.

-sempre adicione/mantenha uma linha em branco no final do arquivo