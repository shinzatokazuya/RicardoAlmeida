import matplotlib.pyplot as plt

# Dados de exemplo
categorias = ['A', 'B', 'C', 'D', 'E']
valores = [3, 5, 7, 9, 11]

# Criar gráficos de barras
plt.bar(categorias, valores)
plt.title('Gráfico de Barras (exemplo)')
plt.xlabel('Categorias')
plt.ylabel('Valores')
plt.grid(True)

plt.savefig('teste.png')

plt.show()
