# Apache Spark, lazy evaluation и многостраничные SQL запросы

Известное об известном: spark работает с “датафреймами” (`dataframes`), которые являются алгоритмами трансформации. Алгоритм запускается в самый последний момент для того, чтобы "дать больше места" оптимизации и за счет оптимизации максимально эффективно его выполнить.

Под катом мы разберем - как можно разложить многостраничный SQL запрос на атомы (без потери эффективности) и как можно за счет этого существенно уменьшить время выполнения ETL конвейера.

# Lazy evaluation

Интересная функциональная особенность spark - `lazy evaluation`: выполнение трансформаций (`transformations`) происходит только в момент выполнения действий (`actions`). Как это работает (грубо): алгоритмы построения датафреймов, предшествующих действию, “склеиваются”, оптимизатор строит наиболее эффективный с его точки зрения итоговый алгоритм, который запускается и дает результат (тот, который был запрошен действием).

Что здесь интересно в контексте нашего изложения: любой сложный запрос можно разложить на “атомы” без потери эффективности. Чуть дальше разберем на примере.

# Многостраничный SQL

Есть много причин, почему мы пишем “многостраничные” SQL запросы, одна из главных, наверное, нежелание создавать промежуточные объекты (нежелание, подкрепленное требованиями эффективности). Ниже приведен пример относительно сложного запроса (конечно, он совсем даже простой, но для целей дальнейшего изложения нам хватит).

```python
qSel = """
select
    con.contract_id as con_contract_id,
    con.begin_date as con_begin_date,
    con.product_id as con_product_id,
    cst.contract_status_type_id as cst_status_type_id,
    sbj.subject_id as sbj_subject_id,
    sbj.subject_name as sbj_subject_name,
    pp.birth_date as pp_birth_date
from
    kasko.contract con
    join kasko.contract_status cst on cst.contract_status_id = con.contract_status_id
    join kasko.subject sbj on sbj.subject_id = con.owner_subject_id
    left join kasko.physical_person pp on pp.subject_id = con.owner_subject_id
"""
dfSel = sp.sql(qSel)
```

Что мы видим:

- данные выбираются из нескольких таблиц 
- используются разные типы join-ов 
- выбираемые колонки распределены по select части, join части (и where части, но здесь ее нет - я ее убрал для простоты)

Этот запрос можно было бы разложить на простые (например, сначала объединить таблицы contract и contract_status, сохранить результат во временную таблицу, потом объединить ее с subject, результат также сохранить во временную таблицу и т.п.). Наверняка при создании реально сложных запросов мы так и делаем, просто потом - после отладки - собираем все это в многостраничную глыбу.

Что здесь плохого? Да ничего, в-сущности, все так работают и к этому привыкли.

Но недостатки - а точнее, что улучшить - есть, читайте дальше.

# Тот же запрос в spark

При использовании spark для трансформации, конечно, можно просто взять и выполнить этот запрос (и будет хорошо, собственно, мы его тоже выполним), но можно пойти другим путем, давайте попробуем.

Давайте разложим этот “сложный” запрос на “атомы” - элементарные датафреймы. Их у нас получится столько, сколько таблиц участвует в запросе (в данном случае - 4).

Вот они - “атомы”:

```python
dfCon = sp.sql("""select
    contract_id as con_contract_id,
    begin_date as con_begin_date,
    product_id as con_product_id,
    owner_subject_id as con_owner_subject_id,
    contract_status_id as con_contract_status_id
from
    kasko.contract""")

dfCStat = sp.sql("""select 
    contract_status_id as cst_status_id,
    contract_status_type_id as cst_status_type_id
from
    kasko.contract_status""")

dfSubj = sp.sql("""select
    subject_id as sbj_subject_id,
    subject_type_id as sbj_subject_type_id,
    subject_name as sbj_subject_name
from
    kasko.subject""")

dfPPers = sp.sql("""select
    subject_id as pp_subject_id,
    birth_date as pp_birth_date
from
    kasko.physical_person""")
```

Объединять их (join) spark позволяет с использованием выражений, отделенных от собственно “атомов”, давайте так и сделаем:

```python
con_stat = f.col("cst_status_id")==f.col("con_contract_status_id")
con_subj_own = f.col("con_owner_subject_id")==f.col("sbj_subject_id")
con_ppers_own = f.col("con_owner_subject_id")==f.col("pp_subject_id")
```

Тогда наш “сложный запрос” будет выглядеть так:

```python
dfAtom = dfCon.join(dfCStat,con_stat, "inner")\
    .join(dfSubj,con_subj_own,"inner") \
    .join(dfPPers,con_ppers_own, "left") \          
    .drop("con_contract_status_id","sbj_subject_type_id",
          "pp_subject_id","con_owner_subject_id","cst_status_id")
```

Что здесь хорошего? На первый взгляд - ничего, даже наоборот: по “сложному” SQL можно понять, что происходит, по нашему “атомарному” запросу - понять сложнее, нужно смотреть на “атомы” и выражения.

Давайте сначала убедимся, что эти запросы эквивалентны - в jupyter книжке по [ссылке](https://github.com/korolmi/dataeng/tree/master/data_doc) я привел планы выполнения обоих запросов (любопытные могут найти 10 отличий, но суть - эквивалентность - налицо). Это, конечно же, никакое не чудо, так и должно быть (см. выше про lazy evaluation и оптимизацию).

Что имеем в итоге - “многостраничный” запрос и “атомарный” запрос работают с одинаковой эффективностью (это важно, без этого дальнейшие рассуждения отчасти теряют смысл).

Ну и теперь давайте найдем хорошее в “атомарном” способе построения запросов.

Что такое “атом” (элементарный датафрейм) - это наши знания о подмножестве предметной области (части реляционной таблицы). Выделяя такие “атомы” мы автоматически (и, что немаловажно, алгоритмически и воспроизводимо) выделяем значимую для нас часть необозримой штуки под названием “физическая модель данных”.

Что такое выражение, которое мы использовали при join? Это также знания о предметной области - именно так (как указано в выражении) связаны между собой сущности предметной области (таблицы в базе данных).

Повторю - это важно - эти “знания” (атомы и выражения) у нас материализованы в исполняемом коде (не в диаграмме или словесном описании), это код, который исполняется при каждом выполнении ETL конвейера (пример взят, кстати, из реальной жизни).

Исполняемый код - как мы с Вами знаем из clean coder - это один из двух объективно существующих артефактов, претендующих на “звание” документация. То есть использование “атомов” позволяет нам сделать шаг вперед в таком важном процессе, как документирование данных.

Что еще хорошего можно найти в “атомарности”?

# Оптимизация конвейеров

В реальной жизни инженера данных - я, кстати, не представился - ETL конвейер состоит из десятков преобразований, подобных вышеприведенному. В них очень часто повторяются таблицы (я как-то посчитал в эксельке - некоторые таблицы используются в 40% запросов).

Что при этом происходит с точки зрения эффективности? Бардак - одна и та же таблица несколько раз читается из источника…

Как это улучшить? В spark есть механизм кэширования датафреймов - мы можем явно указать, какие датафреймы и насколько мы хотим держать в кэше.

Что мы для этого должны сделать - выделить повторяющиеся таблицы и выстроить запросы так, чтобы минимизировать суммарный объем кэша (ибо все таблицы в него по определению “не влезут”, на то и большие данные).

Можно ли это сделать при использовании “многостраничных” SSQ запросов? Да, но… немного сложно (у нас там и датафреймов-то толком нет, только таблицы, их тоже можно кэшировать - сообщество spark над этим работает).

Можно ли это сделать при использовании “атомарных” запросов? Да! И не сложно, нам нужно только обобщить “атомы” - добавить в них колонки, используемые во всех запросах нашего конвейера. Если задуматься, то это “правильно” и с точки зрения документирования: если колонка используется в каком-то запросе (пусть даже в where части), она - часть интересных для нас данных предметной области.

А далее все просто - кэшируем повторяющиеся атомы (датафреймы), выстраиваем цепочку преобразований так, чтобы пересечения кэшируемых датафреймов было минимально (сие не тривиально, но алгоритмизуемо, кстати).

И получаем максимально эффективный конвейер совершенно “бесплатно”. А в придачу к нему полезный и важный артефакт - “заготовку” для документации данных по предметной области.

# Роботизация и автоматизация

Атомы более подвержены автоматической обработке, нежели “великий и могучий SQL” - их структура проста и понятна, парсингом за нас занимается spark (за что ему отдельное спасибо), он же строит планы запросов, анализируя которые можно автоматически переупорядочивать последовательность обработки запросов.

Так что и здесь можно что-то “наиграть”.

# В заключение

Возможно, я слишком оптимистичен - мне представляется, что этот путь (атомизации запросов) является более работающим, нежели попытки постфактум описать зеро данных. В добавок - кстати, что к чему еще “добавок” - получаем прирост в эффективности. Почему атомарный подход я считаю “рабочим”? Он - часть регулярного процесса, а, значит, описанные артефакты имеют реальный шанс быть актуальными и в долгой перспективе.

Я, наверное, что-то упустил - поможете найти (в комментариях)?
