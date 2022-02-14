from filtro_plantao import filtro_plantao, filtro_plantao_new


def test_filtro_plantao():
    expected = filtro_plantao()
    actual = filtro_plantao_new(increment_time=5)
    assert expected is not None, "Função produzindo uma saída nula"
    assert actual == expected, "Função produzindo uma saída diferente"    

